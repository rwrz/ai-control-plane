// Package pricing loads the per-model pricing table from pricing.toml and
// computes per-call cost from provider-reported token counts. Promoted from
// an in-house eval harness pricing package; extended with model-id normalization.
package pricing

import (
	"fmt"
	"regexp"

	"github.com/BurntSushi/toml"
)

// ModelPrice is one [models."xxx"] block in pricing.toml.
//
// A model is priced in exactly one unit, determined by its modality:
//
//	llm  tokens   — *PerMtok, USD per million tokens
//	stt  seconds  — PerAudioSecond, USD per second of audio processed
//	tts  characters — PerCharacter, USD per character synthesised
//
// The exception is OpenAI's gpt-4o-transcribe family: STT billed in tokens, not
// seconds. Those carry *PerMtok despite being stt, which is why the cost helpers
// are selected by the unit the provider actually reported rather than by
// modality.
//
// CachedInputPerMtok defaults to InputPerMtok when omitted.
type ModelPrice struct {
	InputPerMtok       float64 `toml:"input_per_mtok"`
	CachedInputPerMtok float64 `toml:"cached_input_per_mtok"`
	OutputPerMtok      float64 `toml:"output_per_mtok"`

	// TTS rates. Providers publish $/1k characters; per_character exists for
	// anyone quoting the raw rate. Set exactly one.
	PerCharacter    float64 `toml:"per_character"`
	Per1kCharacters float64 `toml:"per_1k_characters"`

	// Audio rates may be written in whichever unit the provider publishes —
	// ElevenLabs quotes $/hour, Deepgram $/minute, LiteLLM $/second. Set exactly
	// one; Load normalizes to seconds and rejects a model that sets more than
	// one, since guessing which was meant would mis-bill every call silently.
	PerAudioSecond float64 `toml:"per_audio_second"`
	PerAudioMinute float64 `toml:"per_audio_minute"`
	PerAudioHour   float64 `toml:"per_audio_hour"`
}

// charPerUnit resolves the TTS rate to USD/character, and reports how many units
// were set so Load can reject an ambiguous entry.
func (p ModelPrice) charPerUnit() (rate float64, unitsSet int) {
	if p.PerCharacter != 0 {
		unitsSet++
		rate = p.PerCharacter
	}
	if p.Per1kCharacters != 0 {
		unitsSet++
		rate = p.Per1kCharacters / 1000
	}
	return rate, unitsSet
}

// audioPerSecond resolves the audio rate to USD/second, and reports how many
// units were set so Load can reject an ambiguous entry.
func (p ModelPrice) audioPerSecond() (rate float64, unitsSet int) {
	for _, u := range []struct {
		value   float64
		perUnit float64 // seconds in that unit
	}{
		{p.PerAudioSecond, 1},
		{p.PerAudioMinute, 60},
		{p.PerAudioHour, 3600},
	} {
		if u.value != 0 {
			unitsSet++
			rate = u.value / u.perUnit
		}
	}
	return rate, unitsSet
}

// Config is the parsed pricing.toml.
type Config struct {
	Models map[string]ModelPrice `toml:"models"`
}

// Load parses pricing.toml. Path can be a full path or relative.
func Load(path string) (*Config, error) {
	var cfg Config
	if _, err := toml.DecodeFile(path, &cfg); err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	// A mis-specified rate is worse than a missing one: a missing model is
	// reported as unpriced, whereas an ambiguous one would bill every call at a
	// rate nobody chose. Fail at load, where it is a deploy error.
	for name, p := range cfg.Models {
		if _, unitsSet := p.audioPerSecond(); unitsSet > 1 {
			return nil, fmt.Errorf(
				"model %q sets more than one audio rate (per_audio_second/minute/hour); set exactly one", name)
		}
		if _, unitsSet := p.charPerUnit(); unitsSet > 1 {
			return nil, fmt.Errorf(
				"model %q sets both per_character and per_1k_characters; set exactly one", name)
		}
	}
	return &cfg, nil
}

// Cost computes the USD cost of one call given provider-reported token counts
// and a model id. Returns (0, false) when the model id has no pricing entry —
// callers must surface that as "unknown", never treat it as free.
//
//	uncached_input = input - cached
//	cost = uncached_input * input_per_mtok / 1M
//	     + cached         * cached_input_per_mtok / 1M
//	     + output         * output_per_mtok / 1M
func (c *Config) Cost(model string, input, cached, output int64) (float64, bool) {
	p, ok := c.Models[model]
	if !ok {
		return 0, false
	}
	cachedRate := p.CachedInputPerMtok
	if cachedRate == 0 {
		cachedRate = p.InputPerMtok
	}
	uncached := input - cached
	if uncached < 0 {
		uncached = 0
	}
	const mtok = 1_000_000.0
	return float64(uncached)*p.InputPerMtok/mtok +
		float64(cached)*cachedRate/mtok +
		float64(output)*p.OutputPerMtok/mtok, true
}

// CostAudioSeconds prices STT billed by audio duration (Whisper, Deepgram,
// ElevenLabs Scribe). Seconds is fractional because every provider reports a
// fractional duration; rounding up to whole seconds would overstate every short
// utterance, and pronunciation checks are mostly short utterances.
//
// Returns (0, false) when the model has no entry OR has one that prices a
// different unit — a token-priced entry cannot answer "what does a second cost",
// and silently returning 0 would book the call as free.
func (c *Config) CostAudioSeconds(model string, seconds float64) (float64, bool) {
	p, ok := c.Models[model]
	if !ok || seconds < 0 {
		return 0, false
	}
	rate, unitsSet := p.audioPerSecond()
	if unitsSet == 0 {
		return 0, false
	}
	return seconds * rate, true
}

// CostCharacters prices TTS billed by input characters (ElevenLabs, Inworld,
// Cartesia). The count is of the text submitted, not of anything returned — the
// response is audio bytes and carries no usage at all.
func (c *Config) CostCharacters(model string, chars int64) (float64, bool) {
	p, ok := c.Models[model]
	if !ok || chars < 0 {
		return 0, false
	}
	rate, unitsSet := p.charPerUnit()
	if unitsSet == 0 {
		return 0, false
	}
	return float64(chars) * rate, true
}

// Has reports whether a model id has a pricing entry.
func (c *Config) Has(model string) bool {
	_, ok := c.Models[model]
	return ok
}

var dateSuffix = regexp.MustCompile(`-\d{4}-\d{2}-\d{2}$`)

// NormalizeModel maps a provider's response model id to a pricing-table key by
// stripping a trailing dated snapshot (e.g. "gpt-4o-mini-2024-07-18" ->
// "gpt-4o-mini"). Gemini's modelVersion ("gemini-2.5-flash") is already a key.
func NormalizeModel(model string) string {
	return dateSuffix.ReplaceAllString(model, "")
}
