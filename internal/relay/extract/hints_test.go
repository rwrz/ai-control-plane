package extract

import "testing"

// The safety property: a hint may fill a gap or replace a relay-side inference,
// but must never overwrite something the provider measured. Without this,
// billing input from a consumer would be a way to understate a bill.
func TestHintsCannotOverwriteMeasuredValues(t *testing.T) {
	t.Run("measured duration wins over a hint", func(t *testing.T) {
		// Deepgram reports a real duration. A caller claiming otherwise —
		// wrongly or deliberately — must not change the bill.
		u := &Usage{Modality: ModalitySTT, AudioSeconds: 30, SecondsAreEstimate: false}
		h, _ := ParseHints([]byte(`{"audio_seconds": 1}`))
		h.Apply(u)

		if u.AudioSeconds != 30 {
			t.Errorf("seconds = %v, want 30 — a hint overwrote a measured value", u.AudioSeconds)
		}
	})

	t.Run("hint replaces a relay-side estimate", func(t *testing.T) {
		// Scribe's duration is reconstructed from word timings and runs ~13%
		// under. The caller decoded the audio and knows the real length.
		u := &Usage{Modality: ModalitySTT, AudioSeconds: 2.739, SecondsAreEstimate: true}
		h, _ := ParseHints([]byte(`{"audio_seconds": 3.16}`))
		h.Apply(u)

		if u.AudioSeconds != 3.16 {
			t.Errorf("seconds = %v, want 3.16", u.AudioSeconds)
		}
		if u.SecondsAreEstimate {
			t.Error("value is now caller-reported and should no longer be flagged an estimate")
		}
	})

	t.Run("hint fills a gap the provider left", func(t *testing.T) {
		u := &Usage{Modality: ModalitySTT}
		h, _ := ParseHints([]byte(`{"audio_seconds": 12.5, "model": "scribe_v1"}`))
		h.Apply(u)

		if u.AudioSeconds != 12.5 || u.Model != "scribe_v1" {
			t.Errorf("got seconds=%v model=%q", u.AudioSeconds, u.Model)
		}
	})

	t.Run("provider model wins over hinted model", func(t *testing.T) {
		u := &Usage{Model: "nova-3"}
		h, _ := ParseHints([]byte(`{"model": "something-else"}`))
		h.Apply(u)

		if u.Model != "nova-3" {
			t.Errorf("model = %q, want nova-3", u.Model)
		}
	})
}

// keyterms is invisible in the response, so the hint is the only way to bill it.
func TestHintsSurcharges(t *testing.T) {
	t.Run("keyterms adds 20 percent", func(t *testing.T) {
		u := &Usage{Modality: ModalitySTT, AudioSeconds: 10, SurchargeMultiplier: 1}
		h, _ := ParseHints([]byte(`{"keyterms":["alpha","beta"]}`))
		h.Apply(u)

		if u.SurchargeMultiplier != 1.20 {
			t.Errorf("multiplier = %v, want 1.20", u.SurchargeMultiplier)
		}
	})

	// Over 100 keyterms imposes a 20-second minimum billable duration.
	t.Run("over 100 keyterms floors duration at 20s", func(t *testing.T) {
		many := `{"keyterms":[`
		for i := 0; i < 101; i++ {
			if i > 0 {
				many += ","
			}
			many += `"t"`
		}
		many += `]}`

		u := &Usage{Modality: ModalitySTT, AudioSeconds: 3, SurchargeMultiplier: 1}
		h, _ := ParseHints([]byte(many))
		h.Apply(u)

		if u.AudioSeconds != 20 {
			t.Errorf("seconds = %v, want the 20s minimum", u.AudioSeconds)
		}
	})

	// The extractor may already have charged for entity detection from the
	// response. Summing both views would bill the same option twice.
	t.Run("response and hint do not double-charge the same option", func(t *testing.T) {
		u := &Usage{Modality: ModalitySTT, AudioSeconds: 10, SurchargeMultiplier: 1.30}
		h, _ := ParseHints([]byte(`{"entity_detection":"pii"}`))
		h.Apply(u)

		if u.SurchargeMultiplier != 1.30 {
			t.Errorf("multiplier = %v, want 1.30, not 1.60", u.SurchargeMultiplier)
		}
	})

	t.Run("entity_detection accepts bool, string and list forms", func(t *testing.T) {
		for _, form := range []string{
			`{"entity_detection":true}`, `{"entity_detection":"pii"}`,
			`{"entity_detection":["pii","phi"]}`,
		} {
			u := &Usage{Modality: ModalitySTT, SurchargeMultiplier: 1}
			h, _ := ParseHints([]byte(form))
			h.Apply(u)
			if u.SurchargeMultiplier != 1.30 {
				t.Errorf("%s -> multiplier %v, want 1.30", form, u.SurchargeMultiplier)
			}
		}
		// ...and that the falsey forms do not charge.
		for _, form := range []string{`{"entity_detection":false}`, `{"entity_detection":""}`, `{}`} {
			u := &Usage{Modality: ModalitySTT, SurchargeMultiplier: 1}
			h, _ := ParseHints([]byte(form))
			h.Apply(u)
			if u.SurchargeMultiplier != 1 {
				t.Errorf("%s -> multiplier %v, want 1", form, u.SurchargeMultiplier)
			}
		}
	})
}

// A malformed or absent header must never fail a relayed call.
func TestHintsTolerateBadInput(t *testing.T) {
	for _, in := range []string{"", "not json", "[1,2,3]"} {
		if _, ok := ParseHints([]byte(in)); ok && in != "[1,2,3]" {
			t.Errorf("ParseHints(%q) reported success", in)
		}
	}
	// Apply on a nil receiver is a no-op, so callers need no guard.
	var h *Hints
	u := &Usage{AudioSeconds: 5}
	h.Apply(u)
	if u.AudioSeconds != 5 {
		t.Error("nil hints mutated the usage")
	}
}
