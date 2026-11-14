# Image generation metering

Built and in production. The agents service is the only consumer that generates
images. This note records how image spend is metered, and why the final design
is much simpler than the first plan.

## The chain being metered

The agents service runs an **LLM-driven fallback chain**: it tries a Gemini
image model first, falls back to OpenAI's gpt-image-1, and runs a second pass
when a content-policy rejection comes back.

| Provider | Endpoint | Model | Billing |
|---|---|---|---|
| Gemini image model | `/v1beta/models/{m}:generateContent` | `gemini-3.1-flash-image` | image-modality output tokens |
| OpenAI | `/v1/images/generations` | `gpt-image-1` | input + image-output tokens |

> An earlier plan also covered Google Imagen, which reports **no usage at all**
> and would have needed per-image pricing by size and quality, fed by caller
> hints. The consumer moved off the deprecated Imagen endpoints before that was
> built. That removed the "provider reports nothing" case and the whole
> per-image pricing matrix.

## The key insight

Both providers bill images **in tokens**, at an image-output rate far above the
text rate (gpt-image-1: $40/1M output tokens; gemini-3.1-flash-image: $60/1M).
Image metering can therefore reuse everything that already exists: the token
fields on `extract.Usage`, the token cost path, the `tokens` unit, and the
existing extractors.

A separate `modality = "image"` exists only so image spend shows up as image in
the dashboard instead of being folded into LLM numbers.

Each image model has its own pricing entry, so its `output_per_mtok` *is* its
image rate. There's no "text rate vs image rate" decision to make at extraction
time. The mispricing the earlier plan worried about could only happen if an
image call were priced against a text model's entry, and that never happens.

## Two usage shapes

**Gemini: `usageMetadata`, same as any Gemini call.** Output tokens are
IMAGE-modality and are broken down in
`candidatesTokensDetails: [{modality:"IMAGE", tokenCount:N}]`. `GeminiExtract`
reads that breakdown. When IMAGE tokens are present, it tags the row `image`.
If the flat `candidatesTokenCount` is ever missing, it recovers the count from
the breakdown, so an image call can never be booked as a $0 text call. Text
completions carry only TEXT details (or none), so the LLM path doesn't change.

**OpenAI: top-level `usage`.** `OpenAIImageExtract`, chosen by path for
`/v1/images/generations`, reads `input_tokens` / `output_tokens`. It handles
two gotchas:

- The response doesn't name the model, so the caller sends
  `X-Usage-Model: gpt-image-1`.
- The response body is megabytes of inline base64. Only the `usage` object is
  kept, never the body.

## Consumer wiring

Relaying is opt-in through `AI_RELAY_URL`. When it isn't set, tools call
providers directly with their own keys.

- **Gemini**: the genai client's `HTTPOptions.BaseURL = {relay}/v1/relay/gemini`.
  The SDK appends `/v1beta/models/…` and the relay strips its own prefix.
  `HTTPOptions.Headers` carries `X-Relay-Key` and `X-Usage-*`.
- **OpenAI**: `POST {relay}/v1/relay/openai/v1/images/generations`, with
  `X-Relay-Key` and `X-Usage-*` instead of `Authorization`.
- **Per-call attribution** comes from the agent framework's tool context
  (which is a `context.Context` exposing user and session). Passing it to the
  provider call instead of `context.Background()` also lets a caller hanging
  up cancel the generation.

## Known gap

Image *input* tokens on a gpt-image-1 **edit** ($10/1M) aren't modelled. The
generation path only sends text input. If edits are ever relayed, their image
input would be priced at the text-input rate. That's an accepted
best-effort gap.
