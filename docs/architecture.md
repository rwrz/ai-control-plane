# Architecture deep dive

The [README](../README.md) explains *what* the service does and *why*. This
document covers *how*: the request lifecycle in code, the WebSocket pump,
shutdown ordering, and the database layer. File references are relative to the
repository root.

## 1. Composition root (`cmd/server/main.go`)

There's no framework and no DI container. `main` wires everything by hand, in
this order:

1. `config.Load()`: env vars, plus optional mounted `.env` bundles (Kubernetes
   secrets). Real environment variables win.
2. `server migrate …`: if this is a migration run, apply the embedded
   migrations and exit (see §6).
3. `pricing.Load()`: parse `pricing.toml`. A model with an ambiguous unit
   (say, both `per_audio_minute` and `per_audio_hour`) is rejected at boot
   rather than mis-billed at runtime.
4. **Pick the sink**: `ledger` (`DB_URL`), `forward` (`RELAY_REPORT_URL`) or
   `slog`. Both together is refused.
5. `relay.New()`: build one `Provider` per upstream and parse the keyring and
   auth mode. Missing provider keys and disabled auth are logged loudly at boot.
6. Mount the routes on one `http.ServeMux`: `/healthz`, `/v1/relay/…`,
   `/tts/…` (Inworld), `POST /v1/usage/events`, and `GET /v1/usage/summary`
   (centre only).
7. Listen, optionally with `SO_REUSEPORT` and a private health listener.

Everything that persists usage hides behind one interface:

```go
type Sink interface {
    Record(context.Context, Event) // must be non-blocking
}
```

The relay never knows whether it's writing to a database or forwarding to
another relay. Sidecar mode came down to adding a second implementation.

## 2. One provider = one configured `ReverseProxy`

`internal/relay/relay.go` declares each provider as data:

```go
{
    Name:           "elevenlabs",
    Target:         "https://api.elevenlabs.io",
    Extract:        elevenLabsExtractorFor,     // response extractor, chosen by path
    ExtractRequest: elevenLabsRequestExtract,   // TTS: bill the submitted text
    StreamMeter:    extract.ElevenLabsStreamMeterFor,  // WebSocket conversations
    StatedCost:     elevenLabsStatedCost,       // provider-quoted price beats the table
    InjectAuth:     injectElevenLabs,           // nil ⇒ unconfigured ⇒ refuse, don't forward empty
}
```

Every hook is **chosen by path** because one host can bill several ways.
OpenAI's `/v1/chat/completions` is LLM tokens, `/v1/audio/transcriptions` is STT
seconds, and `gpt-4o-transcribe` returns a token payload that the LLM extractor
would happily misread as a chat completion.

Adding a provider means adding one entry, an extractor, and pricing rows. There
is no protocol translation layer to extend.

### Request lifecycle (`Provider.ServeHTTP`)

```
refuse if no upstream credential (otherwise the provider's 403 looks like a broken relay)
readAttribution()        header | query | ~path segment  → Meta + credential, all stripped
keyring.Lookup()         enforce: 401 · shadow: log + serve · ok: tenant := key's tenant
isWebSocketUpgrade?  ──► serveWebSocket()   (§3)
makeRewindable()         set GetBody so the transport can retry (see rewind.go)
usageFromRequest()       TTS only: bounded read of the body, restored, stashed in ctx
ReverseProxy.ServeHTTP
  Director:              strip prefix, drop caller creds + Accept-Encoding, InjectAuth
  ModifyResponse:        non-2xx → log upstream error detail, record nothing
                         SSE     → wrap body in a streaming line tee
                         JSON    → bounded read, extract, restore body
                         stamp provider-stated cost, if any
  on body close:         PriceUsage() → Sink.Record()   (async)
```

TTS usage read from the request is only **recorded once the response is
successful**. Synthesis the provider rejected is never billed.

### Extractors (`internal/relay/extract`)

An extractor turns provider bytes into an `extract.Usage`. That struct holds
tokens (input, cached input, output, reasoning), audio seconds, characters, the
model, the modality, an optional provider-stated cost, and a surcharge
multiplier.

- **LLM**: `OpenAIExtract` understands both Chat Completions (`prompt_tokens`)
  and Responses (`input_tokens`, including the streaming `response.completed`
  nesting). Counts are read **through pointers**, so a payload of an unknown
  shape is *rejected* rather than parsed into zeros. `GeminiExtract` reads
  `usageMetadata`, including thoughts, cached content, and tool-use tokens.
- **Image**: Gemini image models report IMAGE-modality output tokens in
  `candidatesTokensDetails`. When those are present the row is tagged `image`,
  and the count is recovered from the breakdown if the flat field is missing.
  `gpt-image-1` returns a top-level `usage` with no model name, so the caller
  sends `X-Usage-Model`. Only the `usage` object is kept, never the megabytes
  of base64.
- **STT**: Whisper's `usage.seconds`, Deepgram's `metadata.duration ×
  channels`, and for ElevenLabs Scribe, which reports no duration, the last
  word timing. That last one is a **floor**, so callers can supply the true
  length as a hint.
- **TTS**: characters (runes) of the submitted text.

### Billing hints (`X-Usage-Request`)

Some billable facts only the caller knows: the true length of audio it decoded,
or a surcharge flag inside a multipart upload too large to buffer. The caller
sends its request parameters as JSON, minus binary data. `extract.Hints.Apply`
enforces one safety rule: **a hint can only fill a gap or replace a value the
relay itself inferred. It can never overwrite a quantity the provider
measured.** A wrong or malicious hint therefore can't understate a bill.
Unknown keys are stored in `metadata`. When a provider adds a billing dimension
later, the relay can learn to read a key consumers already send, with no
consumer redeploy. See [`metering-contract.md`](metering-contract.md).

## 3. WebSocket relaying (`wsproxy.go`, `wsframe.go`)

`ReverseProxy` handles a `101` by copying bytes blindly, so it can't meter
anything. The relay therefore:

1. Hijacks the client connection.
2. Dials the upstream itself (TCP + TLS) and performs the handshake, with the
   real credential injected and hop-by-hop headers rebuilt. `permessage-deflate`
   is removed from the offer, because compressed frames can't be metered. A
   failed dial is retried briefly before the caller gets an error.
3. Relays the upstream's `101` to the client.
4. Runs `pump` in both directions. It reads an RFC 6455 frame **header**,
   writes the header and payload through unchanged, and hands the payload
   (unmasked, if the client masked it) to the provider's `StreamMeter`.
5. On a `protocolError`, meaning a frame the relay can't parse, it switches to
   `io.Copy` for the rest of the conversation. The bytes are still valid for
   both peers; only the relay's *understanding* broke. One unmetered row is
   better than a dropped call.
6. When either side closes, it finalises the meter and records one event per
   conversation.

The relay **observes the protocol and doesn't terminate it**: subprotocols,
close codes, ping/pong timing and fragmentation all pass through as they are.

## 4. Shutdown ordering

The order matters, so `main` spells it out step by step:

```
SIGTERM
 1. draining = true                   /healthz → 503 "draining (N websockets open)"
 2. srv.Shutdown(15s)                 stops accepting; waits for ordinary requests only
                                      (hijacked sockets are invisible to Shutdown)
 3. relay.Drain(SHUTDOWN_GRACE)       waits for live WebSocket conversations to end
 4. close health listener
 5. sink close                        flush ledger buffer / forwarder queue — LAST,
                                      because step 3's sockets were still producing usage
```

If the grace period expires with sockets still open, the service logs an error
with the count. That count is the evidence for raising `SHUTDOWN_GRACE`.

With `LISTEN_REUSE_PORT=true`, a replacement process binds the same port before
step 1 runs, so the port never closes (`internal/infra/netx`). `HEALTH_ADDR`
gives each instance a private address that only it answers on, which lets a
deploy script check that the *new* instance is healthy.

## 5. The two sinks

### Ledger (`internal/ledger`)

- `Record` puts the event on a buffered channel (1024 slots). When the buffer is
  full, it **drops the event and logs it** rather than block a response.
- A background goroutine inserts rows with Bob's typed DSL.
- **Repeated failures are collapsed.** A ledger failure is almost always
  systemic (a schema that lags the binary, a revoked password), so it repeats
  on every event. Logging each one would bury the first occurrence, which is
  the one that explains the problem. Repeats are counted and summarised once a
  minute.

### Forwarder (`internal/forward`)

- A bounded in-memory queue (4096), batches of 50, flushed every 15 s,
  `POST /v1/usage/events` to the centre.
- Bounded retries (~7 s of backoff in total), so a centre that's down can't
  stall shutdown or block the batches queued behind it.
- **One** error log per outage, with a running count of lost events, and a
  `recovered` line when the centre is reachable again.
- It sends quantities, not cost. The centre re-prices against its own table.
  The one exception is a provider-quoted cost, which travels as
  `provider_cost_usd`.
- The wire type `usage.ReportedEvent` is the **single** definition, shared by
  sender and receiver, so the two can't drift. A drifted field wouldn't fail;
  it would arrive as zero and under-bill.

### Ingest (`internal/usage/ingest.go`)

- The tenant comes from the key, never the body.
- Each row is validated separately: one bad row is rejected, the rest are kept.
- `client_event_id` plus a unique index makes retries idempotent.
- `source` defaults to `reported` on the wire. A sender that says nothing has
  measured nothing. (Inside the process, the default is the opposite: an
  in-process event with no source was relayed. The two defaults are opposed on
  purpose.)

## 6. Database layer

- **CockroachDB** (Postgres wire) through **pgx v5** and **Bob**
  (`stephenafamo/bob`) for typed queries. Repositories contain no raw SQL. The
  summary API maps `group_by` through a whitelist of compile-time column names,
  so user input never reaches SQL.
- **Codegen against Postgres, runtime on CockroachDB.** `bobgen-psql` can't
  introspect CockroachDB, so `make bobgen` starts a disposable Postgres, applies
  the same migrations, generates `internal/infra/bob/**`, and tears it down.
  This only works because migrations use **portable DDL** (`TEXT`, `BIGINT`,
  `NUMERIC`, `TIMESTAMPTZ`, `UUID`, `JSONB`; never CockroachDB-only `STRING`).
  The bob library is pinned to the generator's version.
- **golang-migrate, one operation per file**, embedded with `embed.FS` and
  applied by `server migrate`. In production, CI runs that command before every
  deploy, so each migration must work with the code that's *already* running
  (expand, then contract: add the `entity_*` columns, backfill, switch readers,
  drop `session_id`). Migrations 008–012 show that sequence.
- **Schema check at boot.** `db.CheckSchema` compares the database's migration
  version with the newest migration embedded in the binary. If the database is
  behind, every insert would fail, so the service logs a loud error at boot
  instead of letting that show up later as a SQLSTATE in the insert path. It
  doesn't refuse to start, because metering must never take down the relay.
- **No rollup table yet.** Covering indexes on `(tenant, occurred_at)` and
  `(tenant, subject_user, occurred_at)` (and one shaped for the dashboard's
  main panel) keep queries fast at current volume. An OLAP store such as
  ClickHouse is deliberately deferred until volume requires it.

## 7. Deployment shapes

| Shape | Where | How |
|---|---|---|
| Centre (prod + staging) | Kubernetes, behind the existing Traefik v3 | Helm chart, one `IngressRoute` per release, cert-manager TLS; CI runs `server migrate` then deploys |
| Sidecar | a VM next to the voice agent | plain Docker, `--network host`, bound to `127.0.0.1`, `deploy/sidecar/deploy.sh` for zero-downtime handover |

**Why there's no Traefik ForwardAuth.** The original design gated budgets at the
edge with a ForwardAuth middleware. It was dropped. The relay already inspects
every request, so authentication and (future) budget checks can live inside it
and apply the same way to in-cluster and external callers. ForwardAuth also
can't see a credential carried in a WebSocket query string or path segment, so
audio would need an in-service check anyway. Edge rejection would only save a
hop.

## 8. Testing approach

- **Unit tests use real HTTP**: `httptest` upstreams for every provider shape,
  plus a hand-built WebSocket upstream for the frame pump (fragmentation,
  masking, control frames, garbage frames → blind-copy fallback).
- **Each production bug is pinned by a test**: gzip
  (`TestMetersGzippedResponse`), the hint safety rule
  (`TestHintsCannotOverwriteMeasuredValues`), the tenant-from-key rule, the
  cross-language path-segment contract (fixtures produced by the Python
  client), and shutdown draining.
- **The DB layer is checked against a real CockroachDB**, not mocks.
- **The final check is always a real SDK client**, never only `curl`.
