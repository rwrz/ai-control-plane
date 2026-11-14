# Build vs. buy: landscape survey

I evaluated these products in July 2026, before writing any code, against two
hard filters:

1. **Meter our *own* audio provider accounts (TTS/STT) over native WebSocket,
   per end user.** Real-time voice is the largest and least visible part of the
   bill.
2. **Add no new heavy infrastructure** beyond the CockroachDB + Redis-wire cache
   already in operation.

| Product | Best at | Stack | Infra vs CRDB+Redis | Audio + WS | Per-user limits | License / status |
|---|---|---|---|---|---|---|
| Traefik | Edge routing, native WS+SSE | Go | none | WS ✅ / audio ✗ (AI features chat-only, paid Hub) | ✗ (route-level) | MIT; AI = paid |
| LiteLLM | Broadest providers + pricing map | Python | Postgres+Redis | ✅ / partial | ✅ key/team/user/tag | MIT |
| Bifrost | Go performance, Go plugins | Go | Postgres/SQLite | ⚠️ nascent, no Deepgram | virtual key/team | Apache-2.0 open-core |
| Portkey | Battle-tested, fast | TS | Postgres+Redis | ✅ audio+WS in OSS | key/team (VK = enterprise) | MIT · acquired |
| Helicone | Best limit model | Rust gw / TS platform | ❌ ClickHouse+Kafka+… | ✗ | ✅✅ best in class | Apache-2.0 · acquired → maintenance |
| LLMGateway | Simple self-host + dashboard | TS | Postgres+Redis only | ✗ | enterprise/unclear | AGPLv3 + enterprise |
| Envoy AI Gateway | Native token/$ rate limiting | Go | k8s+Envoy+ratelimit svc | ✗ | ✅ (no $ tracking) | Apache-2.0 |
| Cloudflare AI Gateway | Zero-ops, metadata spend limits | SaaS | none (but SaaS) | WS ✅ (Deepgram = *their* hosted instance) | ✅ metadata-scoped | Commercial SaaS |
| KrakenD | Generic Go gateway | Go | none | ✗ both paywalled in CE | ✗ | Apache-2.0 · streaming = enterprise |
| Braintrust | Evals / prompt playground | TS | — | ✗ | ✗ | MIT proxy / SaaS |

Two more were dropped early as the wrong category: a personal-subscription
desktop usage dashboard, and a CLI-subscription-to-API bridge.

## Conclusion

Every option failed at least one filter. Nobody meters the team's *existing*
audio accounts over WebSocket per user, and the products with a real per-user
dashboard at volume require ClickHouse and Kafka. Two of the strongest
candidates were also acquired during the evaluation, which made them a poor
foundation for core cost and enforcement infrastructure.

So the decision was to **build a lean core and borrow ideas**:

- **Helicone**: the limit matrix `subject × {count | tokens | dollars}`.
- **Cloudflare**: WebSocket relay + virtual key + metadata-scoped spend limits;
  the custom-cost override.
- **LiteLLM**: its public `model_prices_and_context_window.json` as a price
  sync source, plus proof that Deepgram and ElevenLabs can be metered.
- **Envoy AI Gateway**: token/$ rate limits keyed on a header.
- **Portkey**: proof that audio and WebSockets can be proxied by an open-source
  gateway; config-as-code routing and fallback.
- **LLMGateway**: proof that a full gateway and dashboard can run on just
  Postgres + Redis.

What actually got built is small: usage extraction, the pricing engine, the
attribution contract, and the audio WebSocket relay that none of these products
offered.
