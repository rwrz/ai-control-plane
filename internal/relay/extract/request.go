package extract

import (
	"encoding/json"
	"unicode/utf8"
)

// RequestExtractor reads usage from a REQUEST body, for providers whose cost is
// determined by what was sent rather than what came back.
//
// TTS is the whole reason this exists: ElevenLabs, Inworld and Cartesia bill per
// character of submitted text and return nothing but audio bytes, so a response
// extractor has literally nothing to read.
//
// The relay only bills a request-derived usage if the call then SUCCEEDS —
// otherwise a rejected request would be charged for synthesis that never
// happened.
type RequestExtractor func(bodyJSON []byte) (*Usage, bool)

// ElevenLabsTTSRequest reads POST /v1/text-to-speech/{voice_id} bodies:
//
//	{"text":"...","model_id":"eleven_multilingual_v2","voice_settings":{...}}
//
// Characters are counted in RUNES, not bytes. ElevenLabs bills per character,
// and a multi-byte character is one character — counting len(text) would
// overcharge every non-ASCII language, which for a language-learning product is
// most of them. "Olá" is 3 characters and 4 bytes.
//
// model_id is optional in the API (ElevenLabs applies a default), so its absence
// is not a rejection — the relay falls back to the caller's X-Usage-Model hint,
// and failing that the call is recorded unpriced rather than dropped.
func ElevenLabsTTSRequest(b []byte) (*Usage, bool) {
	var r struct {
		Text    *string `json:"text"`
		ModelID string  `json:"model_id"`
	}
	if json.Unmarshal(b, &r) != nil || r.Text == nil {
		return nil, false
	}
	return &Usage{
		Model:      r.ModelID,
		Modality:   ModalityTTS,
		Characters: int64(utf8.RuneCountInString(*r.Text)),
		Raw:        json.RawMessage(b),
	}, true
}

// DeepgramSpeakRequest reads POST /v1/speak bodies, which are simply:
//
//	{"text":"..."}
//
// The model is a query parameter rather than a body field, so it is left for the
// relay to take from the caller's X-Usage-Model hint.
func DeepgramSpeakRequest(b []byte) (*Usage, bool) {
	var r struct {
		Text *string `json:"text"`
	}
	if json.Unmarshal(b, &r) != nil || r.Text == nil {
		return nil, false
	}
	return &Usage{
		Modality:   ModalityTTS,
		Characters: int64(utf8.RuneCountInString(*r.Text)),
		Raw:        json.RawMessage(b),
	}, true
}

// InworldTTSRequest reads POST /tts/v1/voice and /tts/v1/voice:stream bodies:
//
//	{"text":"...","voiceId":"Mark","modelId":"inworld-tts-2-flash"}
//
// Inworld streams back JSON chunks carrying base64 audio and reports no usage
// at all, in either the streaming or unary form, so the submitted text is the
// only signal there is.
//
// It matters more than its fallback position suggests: Inworld is the voice agent's
// PRIMARY voice, so until this existed the largest single stream of TTS spend
// was invisible.
func InworldTTSRequest(b []byte) (*Usage, bool) {
	var r struct {
		Text    *string `json:"text"`
		ModelID string  `json:"modelId"`
	}
	if json.Unmarshal(b, &r) != nil || r.Text == nil {
		return nil, false
	}
	return &Usage{
		Model:      r.ModelID,
		Modality:   ModalityTTS,
		Characters: int64(utf8.RuneCountInString(*r.Text)),
		Raw:        json.RawMessage(b),
	}, true
}
