#!/usr/bin/env bash
#
# Zero-downtime deploy of the ai-control-plane sidecar, using plain Docker.
#
# Docker has no rolling-update primitive — `docker run` is not an orchestrator,
# and Compose recreates a container by stopping it first. What makes this work
# without an orchestrator is that the SERVICE supports it: SO_REUSEPORT lets two
# instances hold the same port at once, and the shutdown path drains live
# WebSockets instead of cutting them.
#
#   1. start the new container; it binds the same port and starts taking work
#   2. wait for ITS OWN health address to answer (never the shared port, which
#      either instance may answer)
#   3. signal the old container; it stops accepting, keeps its live audio
#      sockets until they end, then exits
#   4. remove it
#
# The port is bound throughout, so the voice agent never sees a refused connection, and
# no conversation is interrupted.
#
# Rollback is the same command with the previous tag.

set -euo pipefail

IMAGE="${IMAGE:?set IMAGE, e.g. ghcr.io/rwrz/ai-control-plane:v1.3.3}"
ENV_FILE="${ENV_FILE:-/opt/ai-relay/sidecar.env}"
LABEL="ai-relay-sidecar"

# Must exceed the container's SHUTDOWN_GRACE, or Docker SIGKILLs mid-drain and
# undoes the entire point of this script.
STOP_TIMEOUT="${STOP_TIMEOUT:-660}"   # 11m, for SHUTDOWN_GRACE=10m
READY_TIMEOUT="${READY_TIMEOUT:-60}"

# Health ports alternate so the incoming instance never collides with the
# outgoing one, which still holds its own until it exits.
HEALTH_A=127.0.0.1:8091
HEALTH_B=127.0.0.1:8092

log() { printf '  %s\n' "$*"; }
die() { printf 'deploy failed: %s\n' "$*" >&2; exit 1; }

[[ -r "$ENV_FILE" ]] || die "cannot read $ENV_FILE"

old=$(docker ps -q --filter "label=app=$LABEL" | head -1)

# Pick the health port the outgoing instance is NOT using.
old_health=""
if [[ -n "$old" ]]; then
    old_health=$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$old" \
                 | grep '^HEALTH_ADDR=' | cut -d= -f2- || true)
fi
if [[ "$old_health" == "$HEALTH_A" ]]; then new_health=$HEALTH_B; else new_health=$HEALTH_A; fi

log "image        : $IMAGE"
log "outgoing     : ${old:-none} (health ${old_health:-n/a})"
log "incoming     : health $new_health"

# PULL=false uses an image already present locally instead of pulling. For a
# local test on a machine whose arch the registry image was not built for
# (Apple Silicon vs a linux/amd64-only image), build it locally first and set
# PULL=false. Default is to pull, which is what a real deploy wants.
if [[ "${PULL:-true}" == "true" ]]; then
    docker pull "$IMAGE" >/dev/null || die "pull failed"
elif ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    die "PULL=false but image $IMAGE is not present locally; build it first"
fi

name="ai-relay-$(date +%Y%m%d-%H%M%S)"
docker run -d \
    --name "$name" \
    --label "app=$LABEL" \
    --network host \
    --restart unless-stopped \
    --env-file "$ENV_FILE" \
    -e "HEALTH_ADDR=$new_health" \
    "$IMAGE" >/dev/null || die "could not start $name"

log "started      : $name"

# Wait for the NEW instance specifically. Its private health address is the
# only thing that can distinguish it from its predecessor.
ready=false
for _ in $(seq "$READY_TIMEOUT"); do
    if curl -sf -m 2 "http://$new_health/healthz" >/dev/null 2>&1; then ready=true; break; fi
    if ! docker ps -q --no-trunc | grep -q "^$(docker inspect -f '{{.Id}}' "$name")$"; then
        docker logs --tail 30 "$name" >&2
        die "$name exited during startup"
    fi
    sleep 1
done

if [[ "$ready" != true ]]; then
    log "not healthy — rolling back, previous instance keeps serving"
    docker rm -f "$name" >/dev/null 2>&1 || true
    docker logs --tail 30 "$name" >&2 2>/dev/null || true
    die "$name did not become healthy within ${READY_TIMEOUT}s"
fi
log "healthy      : $name"

if [[ -z "$old" ]]; then
    log "no previous instance to retire"
    exit 0
fi

# SIGTERM, then wait. The container stops accepting immediately and holds live
# audio sockets until the conversations end.
log "draining     : $old (up to ${STOP_TIMEOUT}s)"
docker stop -t "$STOP_TIMEOUT" "$old" >/dev/null
docker rm "$old" >/dev/null
log "retired      : $old"

# Keep one previous image for a fast rollback; prune the rest.
docker image prune -f --filter "until=168h" >/dev/null 2>&1 || true

log "done"
