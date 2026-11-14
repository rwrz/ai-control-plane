package extract

import (
	"net/http"
	"testing"
)

// Locks in the billing quantities measured live against each provider. The
// payload shapes here are real responses, trimmed.
func TestAudioExtractors(t *testing.T) {
	// OpenAI returns BOTH duration (audio length) and usage.seconds (what it
	// bills, rounded up). Billing from duration undercounts every short clip.
	t.Run("whisper prefers billable seconds over audio duration", func(t *testing.T) {
		body := []byte(`{"task":"transcribe","language":"english",
			"duration":3.109999895095825,"text":"Hello.",
			"usage":{"type":"duration","seconds":4}}`)

		u, ok := OpenAITranscriptionExtract(body)
		if !ok {
			t.Fatal("expected usage")
		}
		if u.AudioSeconds != 4 {
			t.Errorf("seconds = %v, want 4 (usage.seconds, not duration=3.11)", u.AudioSeconds)
		}
		if u.Modality != ModalitySTT {
			t.Errorf("modality = %q, want stt", u.Modality)
		}
	})

	t.Run("gpt-4o-transcribe bills in tokens", func(t *testing.T) {
		body := []byte(`{"text":"Hello.","usage":{"type":"tokens",
			"input_tokens":14,"output_tokens":6,"total_tokens":20}}`)

		u, ok := OpenAITranscriptionExtract(body)
		if !ok {
			t.Fatal("expected usage")
		}
		if u.InputTokens != 14 || u.OutputTokens != 6 {
			t.Errorf("got %d/%d tokens, want 14/6", u.InputTokens, u.OutputTokens)
		}
		if u.AudioSeconds != 0 {
			t.Errorf("seconds = %v, want 0 — this model is not billed by time", u.AudioSeconds)
		}
	})

	// response_format=json (the SDK default) carries no billable quantity. The
	// call still cost money, so recording it as free would hide real spend.
	t.Run("plain text response is rejected, not billed as free", func(t *testing.T) {
		if u, ok := OpenAITranscriptionExtract([]byte(`{"text":"Hello."}`)); ok {
			t.Errorf("accepted %+v; want rejection so it surfaces as unmetered", u)
		}
	})

	t.Run("deepgram reads duration and model family", func(t *testing.T) {
		body := []byte(`{"metadata":{"duration":3.1608126,"channels":1,
			"model_info":{"2187e11a":{"name":"general-nova-3","arch":"nova-3"}}}}`)

		u, ok := DeepgramExtract(body)
		if !ok {
			t.Fatal("expected usage")
		}
		if u.AudioSeconds != 3.1608126 {
			t.Errorf("seconds = %v", u.AudioSeconds)
		}
		// arch is the pricing key; name ("general-nova-3") is a variant label.
		if u.Model != "nova-3" {
			t.Errorf("model = %q, want nova-3", u.Model)
		}
	})

	// Deepgram bills per channel-second, but duration is wall-clock.
	t.Run("deepgram multiplies stereo by channel count", func(t *testing.T) {
		body := []byte(`{"metadata":{"duration":10,"channels":2,"model_info":{}}}`)

		u, ok := DeepgramExtract(body)
		if !ok {
			t.Fatal("expected usage")
		}
		if u.AudioSeconds != 20 {
			t.Errorf("seconds = %v, want 20 (10s x 2 channels)", u.AudioSeconds)
		}
	})

	// Scribe reports no duration, so length is inferred from word timings. This
	// is a documented UNDERESTIMATE: trailing silence is billed but invisible.
	// Measured live: 2.739s inferred against 3.16s of real audio.
	t.Run("scribe infers length from last word end", func(t *testing.T) {
		body := []byte(`{"language_code":"eng","text":"Hello there",
			"words":[{"text":"Hello","end":1.2},{"text":"there","end":2.739}]}`)

		u, ok := ElevenLabsSTTExtract(body)
		if !ok {
			t.Fatal("expected usage")
		}
		if u.AudioSeconds != 2.739 {
			t.Errorf("seconds = %v, want 2.739", u.AudioSeconds)
		}
	})

	// Characters, not bytes: ElevenLabs bills per character and most of this
	// product's traffic is non-ASCII.
	t.Run("tts counts runes not bytes", func(t *testing.T) {
		u, ok := ElevenLabsTTSRequest([]byte(`{"text":"Olá","model_id":"eleven_v3"}`))
		if !ok {
			t.Fatal("expected usage")
		}
		if u.Characters != 3 {
			t.Errorf("characters = %d, want 3 (\"Olá\" is 3 chars, 4 bytes)", u.Characters)
		}
		if u.Model != "eleven_v3" || u.Modality != ModalityTTS {
			t.Errorf("got model=%q modality=%q", u.Model, u.Modality)
		}
	})
}

// ElevenLabs bills Scribe per channel at full duration, and surcharges some
// options. Three of the four leave a trace in the response; keyterms does not.
func TestScribeChannelsAndSurcharges(t *testing.T) {
	t.Run("multichannel separate bills every channel at full duration", func(t *testing.T) {
		body := []byte(`{"transcripts":[
			{"language_code":"eng","text":"a","words":[{"end":10.0}]},
			{"language_code":"eng","text":"b","words":[{"end":8.0}]}]}`)

		u, ok := ElevenLabsSTTExtract(body)
		if !ok {
			t.Fatal("expected usage")
		}
		if u.AudioSeconds != 20 {
			t.Errorf("seconds = %v, want 20 (10s longest channel x 2 channels)", u.AudioSeconds)
		}
	})

	t.Run("multichannel combined counts distinct channel_index", func(t *testing.T) {
		body := []byte(`{"language_code":"eng","text":"a b","words":[
			{"end":5.0,"channel_index":0},{"end":9.0,"channel_index":1}]}`)

		u, ok := ElevenLabsSTTExtract(body)
		if !ok {
			t.Fatal("expected usage")
		}
		if u.AudioSeconds != 18 {
			t.Errorf("seconds = %v, want 18 (9s x 2 channels)", u.AudioSeconds)
		}
	})

	t.Run("entity detection adds 30 percent", func(t *testing.T) {
		body := []byte(`{"language_code":"eng","text":"hi","words":[{"end":10.0}],
			"entities":[{"text":"Bob","type":"person"}]}`)

		u, _ := ElevenLabsSTTExtract(body)
		if u.SurchargeMultiplier != 1.30 {
			t.Errorf("multiplier = %v, want 1.30", u.SurchargeMultiplier)
		}
	})

	// detect_speaker_roles relabels speakers as agent/customer instead of
	// speaker_0/speaker_1 — the only trace it leaves.
	t.Run("speaker role detection adds 10 percent", func(t *testing.T) {
		body := []byte(`{"language_code":"eng","text":"hi","words":[
			{"end":10.0,"speaker_id":"agent"},{"end":12.0,"speaker_id":"customer"}]}`)

		u, _ := ElevenLabsSTTExtract(body)
		if u.SurchargeMultiplier != 1.10 {
			t.Errorf("multiplier = %v, want 1.10", u.SurchargeMultiplier)
		}
	})

	t.Run("plain diarization is not surcharged", func(t *testing.T) {
		body := []byte(`{"language_code":"eng","text":"hi","words":[
			{"end":10.0,"speaker_id":"speaker_0"},{"end":12.0,"speaker_id":"speaker_1"}]}`)

		u, _ := ElevenLabsSTTExtract(body)
		if u.SurchargeMultiplier != 1 {
			t.Errorf("multiplier = %v, want 1 — diarize alone costs nothing extra", u.SurchargeMultiplier)
		}
	})

	t.Run("surcharges stack", func(t *testing.T) {
		body := []byte(`{"language_code":"eng","text":"hi",
			"words":[{"end":10.0,"speaker_id":"agent"}],
			"entities":[{"text":"Bob"}]}`)

		u, _ := ElevenLabsSTTExtract(body)
		if u.SurchargeMultiplier != 1.40 {
			t.Errorf("multiplier = %v, want 1.40 (1 + 0.30 entities + 0.10 roles)", u.SurchargeMultiplier)
		}
	})
}

// The headers ElevenLabs actually returned on a live Scribe call, verbatim.
func TestElevenLabsStatedCost(t *testing.T) {
	hdr := func(kv ...string) http.Header {
		h := http.Header{}
		for i := 0; i < len(kv); i += 2 {
			h.Set(kv[i], kv[i+1])
		}
		return h
	}

	t.Run("reads a live Scribe response", func(t *testing.T) {
		got, ok := ElevenLabsStatedCost(hdr(
			"character-cost", "1",
			"fiat-cost-before-overages", "0.00018182",
			"fiat-currency", "usd",
		))
		if !ok || got != 0.00018182 {
			t.Errorf("got %v, %v; want 0.00018182, true", got, ok)
		}
	})

	// Zero is a real price: audio short enough to round to no credits costs
	// nothing, and that must stay distinct from "quoted nothing".
	t.Run("zero is a quoted price, not an absence", func(t *testing.T) {
		got, ok := ElevenLabsStatedCost(hdr(
			"character-cost", "0", "fiat-cost-before-overages", "0.0", "fiat-currency", "usd",
		))
		if !ok || got != 0 {
			t.Errorf("got %v, %v; want 0, true", got, ok)
		}
	})

	// Everything that must fall back to the rate table rather than guess.
	for _, tt := range []struct {
		name string
		h    http.Header
	}{
		{"no headers at all", hdr()},
		{"TTS: a credit count but no money", hdr("character-cost", "4")},
		{"currency we cannot add to a USD ledger", hdr(
			"fiat-cost-before-overages", "0.00018182", "fiat-currency", "eur")},
		{"unparseable", hdr("fiat-cost-before-overages", "cheap", "fiat-currency", "usd")},
		{"negative", hdr("fiat-cost-before-overages", "-1", "fiat-currency", "usd")},
		{"NaN", hdr("fiat-cost-before-overages", "NaN", "fiat-currency", "usd")},
		{"Inf", hdr("fiat-cost-before-overages", "Inf", "fiat-currency", "usd")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, ok := ElevenLabsStatedCost(tt.h); ok {
				t.Error("accepted a header set it cannot price from")
			}
		})
	}
}
