package extract

import "encoding/json"

// Hints are the caller's own request parameters, minus any binary payload,
// forwarded in the X-Usage-Request header as JSON.
//
// They exist because some billable facts are knowable only to the consumer:
//
//   - ElevenLabs Scribe bills input audio DURATION but reports none, so the
//     relay can only infer a floor from word timings (~13% under, measured).
//     The consumer decoded that audio and knows its real length.
//   - ElevenLabs surcharges keyterms by 20% and leaves no trace of it in the
//     response. The parameter is in a multipart form wrapping an audio file up
//     to 5GB, which cannot be buffered to read one field.
//   - Whisper's verbose_json names no model.
//
// The contract is deliberately "send what you sent, minus the bytes" rather than
// a fixed list of metering fields. A consumer serialises its request parameters
// once; teaching the relay to bill on a new one is then a relay-side change
// only, and never a redeploy of every caller. Unknown keys are ignored here and
// preserved in Raw, so a parameter the relay does not understand yet still lands
// in the ledger and can be mined later.
//
// Hints are advisory and, by design, cannot inflate a measured quantity — see
// Apply. A consumer that lies or miscounts can only affect quantities the
// provider declined to report.
type Hints struct {
	// Generic quantities, honoured for any provider.
	AudioSeconds *float64 `json:"audio_seconds"`
	Characters   *int64   `json:"characters"`
	Model        string   `json:"model"`

	// ElevenLabs Scribe request parameters that carry a billing consequence.
	Keyterms        []string `json:"keyterms"`
	UseMultiChannel *bool    `json:"use_multi_channel"`
	Channels        *int64   `json:"channels"`
	EntityDetection any      `json:"entity_detection"`
	EntityRedaction any      `json:"entity_redaction"`
	DetectSpkrRoles *bool    `json:"detect_speaker_roles"`

	// Raw is the payload as sent, kept for the ledger's metadata column so a row
	// records exactly what the caller declared.
	Raw json.RawMessage `json:"-"`
}

// ParseHints reads the X-Usage-Request payload. Invalid JSON is not an error
// worth failing a relayed call over — it yields no hints, and the call is
// metered from the response alone.
func ParseHints(b []byte) (*Hints, bool) {
	if len(b) == 0 {
		return nil, false
	}
	var h Hints
	if json.Unmarshal(b, &h) != nil {
		return nil, false
	}
	h.Raw = json.RawMessage(b)
	return &h, true
}

// Apply refines a Usage with caller-declared facts, under one rule: a quantity
// the PROVIDER measured always wins. A hint may only fill a gap the provider
// left, or replace a value the relay itself inferred (SecondsAreEstimate).
//
// That rule is what makes it safe to trust a consumer with billing input. The
// worst a wrong hint can do is mis-state something nobody else reported; it can
// never overwrite Deepgram's duration or OpenAI's token counts.
func (h *Hints) Apply(u *Usage) {
	if h == nil || u == nil {
		return
	}

	if u.Model == "" && h.Model != "" {
		u.Model = h.Model
	}

	// Duration: accept a hint when the provider gave none, or when what we have
	// is our own inference rather than a reported figure.
	if h.AudioSeconds != nil && *h.AudioSeconds > 0 &&
		(u.AudioSeconds == 0 || u.SecondsAreEstimate) {
		u.AudioSeconds = *h.AudioSeconds
		u.SecondsAreEstimate = false

		// A declared duration is per-channel; multichannel bills each channel at
		// the full length. The response-derived path already multiplied, so this
		// only applies to the value just taken from the hint.
		if ch := h.channelCount(); ch > 1 {
			u.AudioSeconds *= float64(ch)
		}
	}

	if h.Characters != nil && *h.Characters > 0 && u.Characters == 0 {
		u.Characters = *h.Characters
	}

	h.applySurcharges(u)
}

// applySurcharges adds the ElevenLabs surcharges that are invisible in the
// response. Surcharges the response DOES reveal are applied by the extractor;
// they are recomputed here from the same basis-point total so a hint and a
// response never double-count the same option.
func (h *Hints) applySurcharges(u *Usage) {
	bps := 0
	if len(h.Keyterms) > 0 {
		bps += 2000 // +20%, undetectable in the response

		// More than 100 keyterms imposes a 20-second minimum billable duration.
		if len(h.Keyterms) > 100 && u.AudioSeconds > 0 && u.AudioSeconds < 20 {
			u.AudioSeconds = 20
		}
	}
	if truthy(h.EntityDetection) || truthy(h.EntityRedaction) {
		bps += 3000 // +30%
	}
	if h.DetectSpkrRoles != nil && *h.DetectSpkrRoles {
		bps += 1000 // +10%
	}
	if bps == 0 {
		return
	}

	// Take the higher of the two views rather than summing them: the extractor
	// may already have charged for entity detection or speaker roles from the
	// response, and adding both would bill the same option twice.
	hinted := 1 + float64(bps)/10000
	if u.SurchargeMultiplier < hinted {
		u.SurchargeMultiplier = hinted
	}
}

// channelCount resolves how many channels were billed. use_multi_channel without
// an explicit count tells us it is multichannel but not how wide; 1 is the
// conservative reading, and the response-derived path handles the real count.
func (h *Hints) channelCount() int64 {
	if h.Channels != nil && *h.Channels > 0 {
		return *h.Channels
	}
	return 1
}

// truthy reports whether an entity_detection/entity_redaction value asks for
// anything. The API accepts a bool, a category string ("pii"), or a list, so any
// non-empty, non-false value means the surcharge applies.
func truthy(v any) bool {
	switch t := v.(type) {
	case nil:
		return false
	case bool:
		return t
	case string:
		return t != ""
	case []any:
		return len(t) > 0
	default:
		return true
	}
}
