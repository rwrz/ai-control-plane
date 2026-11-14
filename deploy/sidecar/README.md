# Running the sidecar

A sidecar is a second copy of this service on the **same machine** as a
latency-sensitive consumer, in production a real-time voice agent. It holds
the provider credentials, relays traffic over loopback, and forwards priced
usage to the central control plane. It has **no database**. See
[Sidecar mode](../../README.md#sidecar-mode-why-a-second-copy-of-the-relay-exists)
for why it exists.

## The two keys

This is the confusing part. The sidecar sits between two authenticated hops and
holds one credential for each. They point in **opposite directions** and can't
be swapped.

```
 voice agent ──── X-Relay-Key ────► SIDECAR ──── X-Relay-Key ────► CENTRE
                  (inbound)                       (outbound)
               RELAY_AUTH_KEYS                 RELAY_REPORT_KEY
              "keys I accept"                 "the key I present"
```

| Variable | Set on | Value | Answers |
|---|---|---|---|
| `RELAY_AUTH_KEYS` | sidecar | `<key A>:voice_agent` | who may use this sidecar |
| `AI_RELAY_KEY` | voice agent | `<key A>` | how the voice agent identifies itself |
| `RELAY_REPORT_KEY` | sidecar | `<key B>` | how the sidecar identifies itself upstream |
| `RELAY_AUTH_KEYS` | centre | must **contain** `<key B>:voice_agent` | who may report usage, and as which tenant |

```bash
openssl rand -hex 32   # key A: sidecar RELAY_AUTH_KEYS + voice agent AI_RELAY_KEY
openssl rand -hex 32   # key B: sidecar RELAY_REPORT_KEY, registered on the centre
```

One key for both hops would technically work. Two keys let you rotate the
voice host's credential without touching the cluster, and revoke a compromised
sidecar without cutting off other consumers.

**The tenant comes from the key at both hops.** The centre books every
forwarded row against the tenant bound to key B and ignores any tenant in the
payload. One sidecar, one tenant.

## Install

```bash
mkdir -p /opt/ai-relay && cd /opt/ai-relay
# copy deploy.sh and sidecar.env.example here
cp sidecar.env.example sidecar.env && chmod 600 sidecar.env
$EDITOR sidecar.env          # both keys, centre URL, provider credentials

IMAGE=ghcr.io/<owner>/ai-control-plane:<tag> ./deploy.sh
```

Point the consumer at it:

```
AI_RELAY_URL=http://127.0.0.1:8080
AI_RELAY_KEY=<key A>
```

Check it:

```bash
curl -s http://127.0.0.1:8091/healthz; curl -s http://127.0.0.1:8092/healthz   # one says ok
docker logs $(docker ps -q --filter label=app=ai-relay-sidecar) | head
#   usage sink: forwarding to central control plane  endpoint=https://…
#   caller authentication enabled  tenants=[voice_agent] mode=enforce
#   listening  addr=127.0.0.1:8080 reuse_port=true
```

⚠️ **`LISTEN_ADDR` must stay on `127.0.0.1`.** With `--network host` there's no
network namespace between the container and the machine, so `0.0.0.0:8080`
would publish a proxy to paid AI accounts on the public IP.

> Why `--network host` at all? On some hosts, Docker bridge networking silently
> hangs on egress (for example, providers that drop private-range traffic). Host
> networking avoids that and keeps the loopback hop truly local.

## Updating with zero downtime

```bash
IMAGE=ghcr.io/<owner>/ai-control-plane:<new-tag> /opt/ai-relay/deploy.sh
```

To roll back, run the same command with the previous tag. To change a key or
credential, edit `sidecar.env` and rerun `deploy.sh` with the current image.
`docker restart` is **not** enough, because `--env-file` is only read when a
container is created.

### Why this works without an orchestrator

Docker has no rolling update. `docker run` isn't an orchestrator, and Compose
stops the old container before starting the new one, which leaves a gap on the
port. On a voice path, that gap is a failed session. Swarm could handle it, but
its routing mesh adds a hop, and the sidecar exists to remove a hop.

The **service** itself makes handover possible:

1. **`SO_REUSEPORT`** (`LISTEN_REUSE_PORT=true`) lets two processes hold
   `127.0.0.1:8080` at the same time. The new container binds while the old one
   is still serving, so the port is never unbound.
2. **WebSocket draining** (`SHUTDOWN_GRACE`). `http.Server.Shutdown` doesn't wait
   for hijacked connections, and every audio socket is hijacked. Before this
   was built, SIGTERM cut every live conversation within a second. Now SIGTERM
   means: stop accepting, keep live sockets until they end, flush usage, exit.

```
1. new container starts, binds :8080 alongside the old    (SO_REUSEPORT)
2. wait for the NEW container's own health address        (not the shared port —
                                                            either instance may answer it)
3. docker stop -t 660 <old>  → SIGTERM
     old closes its listener; new connections go to the new container
     old keeps its live sockets until those conversations end
     old flushes usage and exits
4. docker rm <old>
```

`STOP_TIMEOUT` **must be longer than `SHUTDOWN_GRACE`**. Otherwise Docker sends
SIGKILL mid-drain and the handover breaks. The defaults are 11m and 10m.

### Health ports

`deploy.sh` alternates `HEALTH_ADDR` between `127.0.0.1:8091` and `:8092`. On a
shared main port either instance may answer, so only an address bound by a
single instance can show whether the **new** one is serving. While draining, an
instance answers `503 draining (2 websockets open)`.

### Rehearsing locally

```bash
docker build -t ai-control-plane:local .
cd deploy/sidecar
PULL=false IMAGE=ai-control-plane:local ENV_FILE=./sidecar.env ./deploy.sh
```

Run it twice while polling the listen port. In testing, 500 of 500 polls
succeeded across the handover, and a usage event sent to the sidecar arrived in
the centre's ledger.

## When the centre is unreachable

The sidecar keeps relaying. It logs **one** error per outage with a running
count of lost events (never one per call), and logs `usage forwarding
recovered` when the centre is back. Metering suffers, but the conversation
doesn't.
