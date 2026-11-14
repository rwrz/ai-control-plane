package extract

import "encoding/json"

// Image generation extraction.
//
// Both providers we generate images through bill in TOKENS, not per image, and
// report those tokens in a usage payload — so image reuses the token fields and
// the token cost path. What makes it its own concern is only that the output is
// image-modality tokens, priced far above text output, and must be tagged
// `image` so it is never read as, or reported as, a cheap text completion.
//
// Gemini's "Nano Banana 2" (gemini-3.1-flash-image) reports through the same
// usageMetadata that LLM calls do, so it is handled by GeminiExtract (see
// extract.go), which reads candidatesTokensDetails to recognise the image case.
// OpenAI's gpt-image-1 has its own response shape and its own extractor here.

// OpenAIImageExtract reads /v1/images/generations (gpt-image-1) responses.
//
// It is a separate extractor from OpenAIExtract for two reasons, both of which
// the plain LLM extractor gets wrong:
//
//  1. The modality must be tagged `image`. gpt-image-1's output tokens are image
//     tokens billed at $40/1M — an order of magnitude above any text model's
//     output — so they must never be folded into LLM spend.
//  2. The images response names no model. OpenAIExtract would return a row with
//     an empty model that prices as unpriced for no obvious reason; the
//     documented path is the caller's X-Usage-Model=gpt-image-1 hint, exactly as
//     Whisper's model-less verbose_json already relies on it.
//
// Only the `usage` object is kept in Raw, never the body: an images response is
// megabytes of inline base64 and has no business in the ledger's metadata.
//
// Token fields are read through pointers so an absent field is distinguishable
// from a genuine zero — a `usage` object carrying neither count is rejected
// rather than recorded as a phantom free generation.
func OpenAIImageExtract(b []byte) (*Usage, bool) {
	var env struct {
		Model string          `json:"model"` // usually absent on images responses
		Usage json.RawMessage `json:"usage"`
	}
	if json.Unmarshal(b, &env) != nil || !hasJSONValue(env.Usage) {
		return nil, false
	}

	var u struct {
		// gpt-image-1 uses the Responses-style naming.
		InputTokens  *int64 `json:"input_tokens"`
		OutputTokens *int64 `json:"output_tokens"`
		TotalTokens  int64  `json:"total_tokens"`
	}
	if json.Unmarshal(env.Usage, &u) != nil {
		return nil, false
	}
	if u.InputTokens == nil && u.OutputTokens == nil {
		return nil, false // not a usage payload we understand
	}

	total := u.TotalTokens
	if total == 0 {
		total = deref(u.InputTokens) + deref(u.OutputTokens)
	}
	return &Usage{
		Model:        env.Model,
		Modality:     ModalityImage,
		InputTokens:  deref(u.InputTokens),
		OutputTokens: deref(u.OutputTokens),
		TotalTokens:  total,
		Raw:          env.Usage, // usage only — the body is MBs of base64
	}, true
}
