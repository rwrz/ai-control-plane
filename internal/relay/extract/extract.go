// Package extract pulls normalized token usage out of provider responses —
// streaming (SSE) via a line-scanning tee, or non-streaming from a full body.
// The SSE tee was validated end-to-end against OpenAI + Gemini in spike #2.
package extract

import (
	"bytes"
	"encoding/json"
	"io"
)

// Modality identifies what a call produced, which decides the unit it is billed
// in. Empty means llm, so existing token extractors need no change.
const (
	ModalityLLM = "llm"
	ModalitySTT = "stt"
	ModalityTTS = "tts"
	// ModalityImage is generated images. Both providers we generate through
	// (OpenAI gpt-image-1, Google gemini-3.1-flash-image "Nano Banana 2") bill
	// it in TOKENS — the output is image-modality tokens priced well above text
	// output — so image reuses the token fields below and the token cost path.
	// It is a distinct modality only so image spend is reported as image rather
	// than folded into LLM numbers.
	ModalityImage = "image"
)

// Usage is normalized across providers. OutputTokens includes reasoning/thinking
// tokens (both providers bill them at the output rate).
//
// Exactly one unit is populated per call, per Modality:
//
//	llm    tokens
//	stt    AudioSeconds — except OpenAI's gpt-4o-transcribe family, which bills
//	       STT in tokens and so populates the token fields instead
//	tts    Characters, counted from the REQUEST: a TTS response is audio bytes
//	       and reports no usage at all
//	image  tokens — the output is image-modality tokens (gpt-image-1,
//	       gemini-3.1-flash-image), billed at the model's output rate. Shares the
//	       token fields and the token cost path; a distinct modality only so the
//	       spend reports as image rather than folded into LLM
type Usage struct {
	Model             string
	Modality          string
	InputTokens       int64
	CachedInputTokens int64
	OutputTokens      int64
	TotalTokens       int64
	AudioSeconds      float64
	Characters        int64
	// SurchargeMultiplier scales the computed cost for provider options that
	// bill above the base rate (ElevenLabs charges +30% for entity detection,
	// +10% for speaker-role detection). 0 is treated as 1 so extractors that
	// have no surcharges need not set it.
	SurchargeMultiplier float64
	// SecondsAreEstimate marks AudioSeconds as inferred by the relay rather than
	// reported by the provider — currently only Scribe, whose duration is
	// reconstructed from word timings and is a floor. A caller-supplied hint may
	// replace an estimate but never a measured value.
	SecondsAreEstimate bool
	// ProviderCostUSD is the price the PROVIDER itself stated for this call,
	// for the few that say. It outranks anything computed from pricing.toml:
	// it is the provider's own arithmetic over its own quantities, on the
	// account that will be invoiced, and it already includes surcharges the
	// response does not otherwise reveal.
	//
	// ElevenLabs Scribe is the case that motivated it. Its billable duration is
	// the audio file, quantised into whole credits, while the relay can only
	// reconstruct a floor from word timings — and keyterms add 20% that leaves
	// no trace at all. Measured against a live session the reconstruction ran
	// ~27% under; the stated figure is exact.
	//
	// A POINTER because zero is a real answer: audio short enough to round to
	// no credits genuinely costs nothing, and that is different from a provider
	// that never quoted a price.
	ProviderCostUSD *float64
	Raw             json.RawMessage
}

// Extractor parses one JSON object (an SSE data line, or a full response body)
// and returns Usage if the object carries a usage payload.
type Extractor func(objJSON []byte) (*Usage, bool)

// OpenAIExtract reads usage from both OpenAI APIs, which name the fields
// differently:
//
//	Responses (current)       usage.input_tokens  / output_tokens
//	                          usage.input_tokens_details.cached_tokens
//	Chat Completions (legacy) usage.prompt_tokens / completion_tokens
//	                          usage.prompt_tokens_details.cached_tokens
//
// Responses is the API to meter correctly: it is where OpenAI is investing and
// Chat Completions is in legacy mode, headed for deprecation. Chat Completions
// support stays until the last caller is migrated.
//
// It also handles the Responses streaming shape, where the terminal
// "response.completed" event nests the payload under `response` rather than
// carrying it at the top level.
//
// Requires stream_options.include_usage=true when streaming Chat Completions.
//
// Token fields are read through pointers so an absent field is distinguishable
// from a genuine zero: a `usage` object matching neither naming convention is
// rejected outright. Unmarshalling it into flat ints would instead succeed with
// every count at zero and silently record a phantom free call.
func OpenAIExtract(b []byte) (*Usage, bool) {
	var env struct {
		Model string          `json:"model"`
		Usage json.RawMessage `json:"usage"`
		// Streaming Responses API: the final event nests everything under
		// `response`. Earlier events carry `"usage": null` here.
		Response *struct {
			Model string          `json:"model"`
			Usage json.RawMessage `json:"usage"`
		} `json:"response"`
	}
	if json.Unmarshal(b, &env) != nil {
		return nil, false
	}

	model, raw := env.Model, env.Usage
	if !hasJSONValue(raw) && env.Response != nil {
		model, raw = env.Response.Model, env.Response.Usage
	}
	if !hasJSONValue(raw) {
		return nil, false
	}

	var u struct {
		// Responses naming (current).
		InputTokens        *int64 `json:"input_tokens"`
		OutputTokens       *int64 `json:"output_tokens"`
		InputTokensDetails struct {
			CachedTokens int64 `json:"cached_tokens"`
		} `json:"input_tokens_details"`

		// Chat Completions naming (legacy).
		PromptTokens        *int64 `json:"prompt_tokens"`
		CompletionTokens    *int64 `json:"completion_tokens"`
		PromptTokensDetails struct {
			CachedTokens int64 `json:"cached_tokens"`
		} `json:"prompt_tokens_details"`

		TotalTokens int64 `json:"total_tokens"`
	}
	if json.Unmarshal(raw, &u) != nil {
		return nil, false
	}

	input, gotInput := firstSet(u.InputTokens, u.PromptTokens)
	output, gotOutput := firstSet(u.OutputTokens, u.CompletionTokens)
	if !gotInput && !gotOutput {
		return nil, false // not a usage payload we understand
	}

	// Exactly one naming convention is present, so the other's details block is
	// zero and the sum is just that convention's cached count.
	cached := u.PromptTokensDetails.CachedTokens + u.InputTokensDetails.CachedTokens

	return &Usage{
		Model:             model,
		InputTokens:       input,
		CachedInputTokens: cached,
		OutputTokens:      output, // already includes reasoning tokens
		TotalTokens:       u.TotalTokens,
		Raw:               raw,
	}, true
}

// hasJSONValue reports whether raw holds something other than absent/null.
func hasJSONValue(raw json.RawMessage) bool {
	return len(raw) > 0 && string(raw) != "null"
}

// firstSet returns the first non-nil value and whether one was found.
func firstSet(values ...*int64) (int64, bool) {
	for _, v := range values {
		if v != nil {
			return *v, true
		}
	}
	return 0, false
}

// GeminiExtract reads the top-level `usageMetadata` object.
//
// Gemini splits a call's tokens across five counts, and billing needs all of
// them — the two obvious ones alone undercount badly:
//
//	promptTokenCount         input, and INCLUDES cachedContentTokenCount
//	cachedContentTokenCount  the cached subset of the prompt, billed at the
//	                         discounted cache-read rate (Config.Cost subtracts
//	                         it from input rather than charging it twice)
//	candidatesTokenCount     the visible answer
//	thoughtsTokenCount       reasoning tokens — invisible in the response body
//	                         but billed at the full output rate. On 2.5-flash
//	                         these routinely dwarf the answer: a live call
//	                         measured 17 candidate tokens against 1121 thought
//	                         tokens, so ignoring them undercounts it by ~98%.
//	toolUsePromptTokenCount  prompt tokens spent on tool use (e.g. Search
//	                         grounding). NOT part of promptTokenCount — it is a
//	                         separate addend, so it is added to input here.
//
// With those, input+output reconciles against totalTokenCount.
//
// Counts are read through pointers so an absent field is distinguishable from a
// genuine zero. Every field is `omitempty` in the API, so a usageMetadata object
// carrying none of them (a shape change, or one holding only trafficType) would
// otherwise unmarshal to all-zeros and report success — recording a phantom
// free call instead of surfacing that we no longer understand the payload.
func GeminiExtract(b []byte) (*Usage, bool) {
	var env struct {
		ModelVersion  string          `json:"modelVersion"`
		UsageMetadata json.RawMessage `json:"usageMetadata"`
	}
	if json.Unmarshal(b, &env) != nil || len(env.UsageMetadata) == 0 {
		return nil, false
	}
	var m struct {
		PromptTokenCount        *int64 `json:"promptTokenCount"`
		CandidatesTokenCount    *int64 `json:"candidatesTokenCount"`
		TotalTokenCount         *int64 `json:"totalTokenCount"`
		CachedContentTokenCount *int64 `json:"cachedContentTokenCount"`
		ThoughtsTokenCount      *int64 `json:"thoughtsTokenCount"`
		ToolUsePromptTokenCount *int64 `json:"toolUsePromptTokenCount"`
		// Output tokens split by modality. An image generation
		// (gemini-3.1-flash-image, "Nano Banana 2") returns its output as
		// IMAGE-modality tokens, billed an order of magnitude above text output.
		// Reading this is what lets an image call be tagged and priced as image
		// rather than silently as a cheap text completion — the dangerous case,
		// because the flat candidatesTokenCount alone looks perfectly plausible.
		CandidatesTokensDetails []struct {
			Modality   string `json:"modality"`
			TokenCount int64  `json:"tokenCount"`
		} `json:"candidatesTokensDetails"`
	}
	if json.Unmarshal(env.UsageMetadata, &m) != nil {
		return nil, false
	}
	// Recognise the payload by the presence of any count, not by a non-zero
	// value: a legitimately empty prompt still deserves an event.
	if m.PromptTokenCount == nil && m.CandidatesTokenCount == nil &&
		m.TotalTokenCount == nil && m.ThoughtsTokenCount == nil {
		return nil, false
	}

	output := deref(m.CandidatesTokenCount) + deref(m.ThoughtsTokenCount)

	// Image generation: the visible output is IMAGE-modality tokens. Tag the
	// modality so the row prices against the model's image-output rate and reports
	// as image; and, defensively, recover the count from the modality breakdown
	// if the flat candidatesTokenCount is ever absent, so an image call can never
	// book as a $0 text call. A text completion carries only TEXT-modality
	// details (or none), so this leaves the LLM path byte-for-byte unchanged.
	modality := ""
	var imageTokens int64
	for _, d := range m.CandidatesTokensDetails {
		if d.Modality == "IMAGE" {
			imageTokens += d.TokenCount
		}
	}
	if imageTokens > 0 {
		modality = ModalityImage
		if output == 0 {
			output = imageTokens
		}
	}

	return &Usage{
		Model:             env.ModelVersion,
		Modality:          modality,
		InputTokens:       deref(m.PromptTokenCount) + deref(m.ToolUsePromptTokenCount),
		CachedInputTokens: deref(m.CachedContentTokenCount),
		OutputTokens:      output,
		TotalTokens:       deref(m.TotalTokenCount),
		Raw:               env.UsageMetadata,
	}, true
}

// deref reads an optional count, treating absent as zero.
func deref(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}

// sseTee wraps a streaming response body: bytes pass through to the caller
// unchanged (no full-body buffering, no added latency) while complete SSE lines
// are scanned for usage. onDone fires once, at EOF, with the last usage seen
// (nil if none) — the final chunk is authoritative.
type sseTee struct {
	src     io.ReadCloser
	extract Extractor
	onDone  func(*Usage)
	pend    []byte
	latest  *Usage
	done    bool
}

// NewSSETee returns a body wrapper that extracts usage while streaming through.
func NewSSETee(src io.ReadCloser, ex Extractor, onDone func(*Usage)) io.ReadCloser {
	return &sseTee{src: src, extract: ex, onDone: onDone}
}

func (t *sseTee) Read(p []byte) (int, error) {
	n, err := t.src.Read(p)
	if n > 0 {
		t.pend = append(t.pend, p[:n]...)
		t.drain()
	}
	if err == io.EOF && !t.done {
		t.done = true
		t.onDone(t.latest)
	}
	return n, err
}

func (t *sseTee) drain() {
	for {
		i := bytes.IndexByte(t.pend, '\n')
		if i < 0 {
			return
		}
		line := bytes.TrimSpace(t.pend[:i])
		t.pend = t.pend[i+1:]
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		data := bytes.TrimSpace(line[len("data:"):])
		if len(data) == 0 || bytes.Equal(data, []byte("[DONE]")) {
			continue
		}
		if u, ok := t.extract(data); ok {
			t.latest = u
		}
	}
}

func (t *sseTee) Close() error {
	if !t.done {
		t.done = true
		t.onDone(t.latest)
	}
	return t.src.Close()
}
