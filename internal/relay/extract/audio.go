package extract

import (
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"strings"
)

// Audio extraction, for the STT and TTS providers.
//
// Two things make this harder than LLM extraction, and both shape the code
// below:
//
//  1. Not every response names its model. Whisper's verbose_json does not, and
//     a TTS response is raw audio with no JSON at all. Where the response is
//     silent the relay falls back to the caller-declared X-Usage-Model hint, so
//     these extractors leave Model empty rather than inventing one.
//  2. TTS cost is driven by the REQUEST. The text submitted is the billable
//     quantity and nothing comes back but audio bytes, so TTS is handled by a
//     RequestExtractor (see request.go) instead of an Extractor.

// OpenAITranscriptionExtract reads /v1/audio/transcriptions responses, which
// come in two billing shapes depending on the model:
//
//	whisper-1          "usage":{"type":"duration","seconds":N} → billed by time
//	gpt-4o-transcribe  "usage":{"type":"tokens",...}           → billed by tokens
//
// `usage` is read BEFORE the top-level `duration`, and the order matters. A
// verbose_json response carries both, and they disagree: measured live, a clip
// reported duration=3.109999895 alongside usage.seconds=4. `duration` is the
// length of the audio; usage.seconds is what OpenAI actually charges for, having
// rounded up. Billing from `duration` would undercount that call by 29%, and
// short clips — every pronunciation check — are where the rounding hurts most.
// Prefer the provider's own billable figure over anything derived.
//
// Neither names the model, so Model is left for the relay to fill from the hint.
//
// A plain `{"text":"..."}` (response_format=json, the SDK default) carries no
// billable quantity at all. That is rejected rather than recorded as free: the
// call really did cost money and booking $0 would hide it. It surfaces instead
// as an unpriced call, which is visible.
func OpenAITranscriptionExtract(b []byte) (*Usage, bool) {
	var r struct {
		Text     *string  `json:"text"`
		Duration *float64 `json:"duration"`
		Usage    *struct {
			Type         string `json:"type"`
			InputTokens  *int64 `json:"input_tokens"`
			OutputTokens *int64 `json:"output_tokens"`
			TotalTokens  *int64 `json:"total_tokens"`
			// The seconds variant, returned by gpt-4o-transcribe when billing by
			// duration rather than tokens.
			Seconds *float64 `json:"seconds"`
		} `json:"usage"`
	}
	if json.Unmarshal(b, &r) != nil {
		return nil, false
	}
	// Guard against matching an unrelated JSON body that happens to parse.
	if r.Text == nil && r.Duration == nil && r.Usage == nil {
		return nil, false
	}

	u := &Usage{Modality: ModalitySTT, Raw: json.RawMessage(b)}

	if r.Usage != nil {
		if r.Usage.Seconds != nil {
			u.AudioSeconds = *r.Usage.Seconds
			return u, true
		}
		if r.Usage.InputTokens != nil || r.Usage.OutputTokens != nil {
			u.InputTokens = deref(r.Usage.InputTokens)
			u.OutputTokens = deref(r.Usage.OutputTokens)
			u.TotalTokens = deref(r.Usage.TotalTokens)
			if u.TotalTokens == 0 {
				u.TotalTokens = u.InputTokens + u.OutputTokens
			}
			return u, true
		}
	}
	if r.Duration != nil {
		u.AudioSeconds = *r.Duration
		return u, true
	}
	return nil, false
}

// DeepgramExtract reads Deepgram's pre-recorded and live transcription
// responses. Both carry a metadata block:
//
//	{"metadata":{"duration":17.56,"channels":1,
//	             "model_info":{"<uuid>":{"name":"...","arch":"nova-3"}}}, ...}
//
// duration is the billable quantity, in seconds.
//
// The model is reported indirectly, keyed by an opaque uuid. "arch" is the
// family ("nova-3"), which is what pricing keys on; "name" is a variant label.
// Only the first entry is read — a request runs one model, and a map here is an
// artefact of how Deepgram serialises it.
//
// Multichannel caveat: Deepgram bills per channel-second on multichannel audio,
// but duration reports wall-clock seconds. Callers sending channels > 1 would be
// undercounted, so that is surfaced rather than silently mis-billed.
func DeepgramExtract(b []byte) (*Usage, bool) {
	var r struct {
		Metadata *struct {
			Duration  *float64 `json:"duration"`
			Channels  *int64   `json:"channels"`
			ModelInfo map[string]struct {
				Name string `json:"name"`
				Arch string `json:"arch"`
			} `json:"model_info"`
		} `json:"metadata"`
	}
	if json.Unmarshal(b, &r) != nil || r.Metadata == nil || r.Metadata.Duration == nil {
		return nil, false
	}

	u := &Usage{
		Modality:     ModalitySTT,
		AudioSeconds: *r.Metadata.Duration,
		Raw:          json.RawMessage(b),
	}
	// Bill every channel: two channels of a 60s call is 120 channel-seconds.
	if ch := deref(r.Metadata.Channels); ch > 1 {
		u.AudioSeconds *= float64(ch)
	}
	for _, info := range r.Metadata.ModelInfo {
		if info.Arch != "" {
			u.Model = info.Arch
		} else {
			u.Model = info.Name
		}
		break
	}
	return u, true
}

// ElevenLabsSTTExtract reads /v1/speech-to-text (Scribe) responses.
//
// ElevenLabs bills Scribe on the INPUT AUDIO duration, which the response does
// not report. The transcript's word timings are the only available signal, so
// billable length is taken as the end of the last timed word.
//
// That is a FLOOR, not the figure ElevenLabs charges: trailing silence after the
// final word is billed and invisible here. Measured live, a 3.16s clip inferred
// 2.739s — about 13% under. Reconcile Scribe against the invoice before treating
// it as authoritative. Metering it low still beats not metering it.
//
// Three of ElevenLabs' four billing surcharges leave a trace in the response and
// are applied here; the fourth does not and cannot be:
//
//	use_multi_channel     each channel is billed at the FULL audio duration, so
//	                      cost scales linearly with channel count
//	entity_detection      +30%, visible as an "entities" field
//	entity_redaction      +30%, same surcharge, same field family
//	detect_speaker_roles  +10%, visible as speaker_id "agent"/"customer" rather
//	                      than "speaker_0"/"speaker_1"
//	keyterms              +20%, INVISIBLE in the response. A caller enabling
//	                      keyterms is undercounted by 20% and nothing here can
//	                      detect it — it would need the request's multipart form,
//	                      which carries an audio file up to 5GB and cannot be
//	                      buffered to read one field.
//
// diarize on its own costs nothing extra.
//
// The surcharges above only decide the multiplier when the response is all we
// have. In the relay it usually is not: ElevenLabs states the finished price in
// a response HEADER, which ElevenLabsStatedCost reads and PriceUsage prefers
// outright — including the keyterm premium this comment calls undetectable, and
// including the credit quantisation the seconds below cannot express. So the
// multiplier is the fallback path, not the normal one, and it must stay correct
// for the case where no header arrives rather than be treated as dead.
func ElevenLabsSTTExtract(b []byte) (*Usage, bool) {
	// Single-channel and multichannel_output_style=combined share a shape;
	// style=separate nests one transcript per channel under "transcripts".
	type transcript struct {
		LanguageCode *string           `json:"language_code"`
		Text         *string           `json:"text"`
		Entities     []json.RawMessage `json:"entities"`
		Words        []struct {
			End          *float64 `json:"end"`
			SpeakerID    string   `json:"speaker_id"`
			ChannelIndex *int64   `json:"channel_index"`
		} `json:"words"`
	}
	var r struct {
		transcript
		Transcripts []transcript `json:"transcripts"`
	}
	if json.Unmarshal(b, &r) != nil {
		return nil, false
	}

	parts := r.Transcripts
	if len(parts) == 0 {
		// language_code is Scribe-specific and distinguishes this from other
		// providers' transcript JSON.
		if r.LanguageCode == nil || r.Text == nil {
			return nil, false
		}
		parts = []transcript{r.transcript}
	}

	var (
		seconds     float64
		hasEntities bool
		hasRoles    bool
		channels    = map[int64]struct{}{}
	)
	for i, p := range parts {
		if len(p.Entities) > 0 {
			hasEntities = true
		}
		// style=separate: one transcript per channel. style=combined: a single
		// transcript whose words carry channel_index.
		channels[int64(i)] = struct{}{}
		for _, w := range p.Words {
			// Every channel is billed at the full duration, so the billable
			// length is the longest channel, multiplied by channel count below.
			if w.End != nil && *w.End > seconds {
				seconds = *w.End
			}
			if w.SpeakerID == "agent" || w.SpeakerID == "customer" {
				hasRoles = true
			}
			if w.ChannelIndex != nil {
				channels[*w.ChannelIndex] = struct{}{}
			}
		}
	}

	// Accumulate in basis points and divide once. Adding 0.30 and 0.10 to a
	// float yields 1.4000000000000001; percentages of a bill deserve exact
	// arithmetic even when the error is currently negligible.
	surchargeBps := 0
	if hasEntities {
		surchargeBps += 3000 // entity_detection / entity_redaction: +30%
	}
	if hasRoles {
		surchargeBps += 1000 // detect_speaker_roles: +10%
	}

	return &Usage{
		Modality:     ModalitySTT,
		AudioSeconds: seconds * float64(len(channels)),
		// Reconstructed from word timings, not reported: a caller that knows the
		// real input duration may override this.
		SecondsAreEstimate:  true,
		SurchargeMultiplier: 1 + float64(surchargeBps)/10000,
		Raw:                 json.RawMessage(b),
	}, true
}

// ElevenLabsStatedCost reads the price ElevenLabs puts on a Scribe response.
//
// Scribe answers with headers naming what the call cost on this account:
//
//	character-cost:            1
//	fiat-cost-before-overages: 0.00018182
//	fiat-currency:             usd
//
// Both are on ElevenLabs' own access-control-expose-headers list, so they are
// a documented part of the response rather than something observed by accident.
//
// This beats reconstructing the price for three reasons, all measured against
// live calls:
//
//   - ElevenLabs bills whole CREDITS of ~2.9752s each, not the raw duration.
//     Across seven clips from 0.46s to 70s the credit count was always
//     round(duration / 2.9752), so a short utterance — most of a coaching
//     session — is quantised in a direction the second-count cannot know.
//   - keyterms surcharge 20% and are INVISIBLE in the body (see the comment on
//     ElevenLabsSTTExtract). The stated figure includes them: the same 12s clip
//     costs 4 credits plain and 5 with one keyterm.
//   - the duration itself is reconstructed from word timings and is a floor,
//     short by the padding and trailing silence in every VAD segment.
//
// The caveat is in the name: BEFORE overages. Once an account's included
// credits are spent the invoice may exceed this, so it is the provider's price
// at plan rates, not a guaranteed invoice line. It is still far closer than the
// reconstruction it replaces.
//
// Only USD is accepted. A cost in another currency is a number we cannot add to
// a USD ledger, and converting it would invent an exchange rate nobody chose.
func ElevenLabsStatedCost(h http.Header) (float64, bool) {
	if !strings.EqualFold(strings.TrimSpace(h.Get("fiat-currency")), "usd") {
		return 0, false
	}
	raw := strings.TrimSpace(h.Get("fiat-cost-before-overages"))
	if raw == "" {
		return 0, false
	}
	cost, err := strconv.ParseFloat(raw, 64)
	if err != nil || cost < 0 || math.IsNaN(cost) || math.IsInf(cost, 0) {
		return 0, false
	}
	return cost, true
}
