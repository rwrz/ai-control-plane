# The metering contract

What a consumer sends the relay so its spend can be attributed and priced. The
goal is that a consumer writes to this **once** and never changes again: new
billing dimensions are taught to the relay, not pushed out to every caller.

## Headers

All are optional and all are stripped before the request reaches the provider.

| Header | Purpose |
|---|---|
| `X-Usage-Tenant` | who owns the spend (`backend`, `voice_agent`). Ignored when auth is on: the key decides |
| `X-Usage-Module` | which service made the call (`dictionary`, `pronunciation`) |
| `X-Usage-Feature` | finer label within the module (`term_generation`) |
| `X-Usage-User` | the end user the call is on behalf of |
| `X-Usage-Entity-Type` | what kind of unit of work the call belongs to (`session`, `agent_session`, `channel`) — low-cardinality, the dashboard filters on it |
| `X-Usage-Entity-Id` | which one; every call carrying the same pair is summed as one unit (a coaching session, a call's transcription plus its reports) |
| `X-Usage-Session` | **deprecated** — read as entity `("session", id)` for consumers not yet upgraded; an explicit entity wins. Removed once no consumer sends it |
| `X-Usage-Model` | model id, **only** when the response won't name one |
| `X-Usage-Request` | the request's parameters as JSON, minus binary payloads |

## `X-Usage-Request` — "what you sent, minus the bytes"

Some billable facts are knowable only to the caller:

- **ElevenLabs Scribe** bills on input audio duration and reports none. The
  relay reconstructs it from word timings, which runs ~13% under (measured:
  2.739s inferred against 3.16s of real audio). The caller decoded that audio
  and knows its true length.
- **ElevenLabs `keyterms`** adds a 20% surcharge and leaves no trace in the
  response. It lives in a multipart form wrapping an audio file up to 5GB, which
  cannot be buffered to read one field.
- **Whisper's** `verbose_json` names no model.

So send the parameters you already built, serialised, with any file/audio field
omitted:

```http
X-Usage-Request: {"model_id":"scribe_v1","audio_seconds":12.4,"keyterms":["mitosis"],"diarize":true}
```

**Unknown keys are kept, not rejected.** They are stored in
`usage_events.metadata` and can be mined later. That is the extension point:
when a provider adds a billing dimension, the relay learns to read a key you are
already sending — no consumer redeploy.

### Keys the relay currently bills on

| Key | Effect |
|---|---|
| `audio_seconds` | STT duration, when the provider reports none |
| `characters` | TTS character count, when not derivable from the body |
| `model` | model id, when the response omits it |
| `channels` | multichannel STT — each channel bills at full duration |
| `keyterms` | ElevenLabs +20%; >100 terms also floors duration at 20s |
| `entity_detection`, `entity_redaction` | ElevenLabs +30% (bool, string or list) |
| `detect_speaker_roles` | ElevenLabs +10% |

### The safety rule

**A hint can never overwrite a quantity the provider measured.** It may only

1. fill a gap the provider left, or
2. replace a value the *relay itself inferred* (currently only Scribe's duration).

This is what makes it safe to accept billing input from a consumer: a wrong or
malicious hint cannot understate a bill that Deepgram's reported duration or
OpenAI's token counts already established. It is enforced in
`extract.Hints.Apply` and pinned by `TestHintsCannotOverwriteMeasuredValues`.

A hint also cannot double-charge a surcharge the response already revealed — the
two views are reconciled by taking the higher, not by summing.

## What is measured vs. inferred

Trust these differently when reconciling against an invoice.

| Provider | Quantity | Source |
|---|---|---|
| OpenAI (LLM) | tokens | reported |
| Gemini | tokens incl. thoughts, cached, tool-use | reported |
| OpenAI Whisper | `usage.seconds` — what OpenAI charges, rounded up | reported |
| OpenAI gpt-4o-transcribe | tokens | reported |
| Deepgram | `metadata.duration` × channels | reported |
| ElevenLabs TTS | characters in the request text (runes) | derived from request |
| **ElevenLabs Scribe** | **last word timing — a FLOOR** | **inferred; send `audio_seconds`** |

Only the Scribe row is approximate, and only until a caller supplies
`audio_seconds`. Rows carrying a hint keep it verbatim in `metadata`, so a cost
derived from a claim can always be audited against the claim.

## Unpriced is not free

A model with no entry in `pricing.toml` records `cost_usd = 0` with
`cost_known = false` and logs a warning. Never read a 0 without checking
`cost_known` — `usage.Totals` reports `UnpricedCalls` for exactly this reason.
