# ai-control-plane

MIGRATE_PATH := db/migrations
BOB_VERSION  := v0.48.0   # bobgen generator AND the bob library must match (see go.mod)

# Runtime DB — CockroachDB (or Postgres). Export DB_URL, e.g.
#   export DB_URL='postgresql://root@localhost:26257/aicontrolplane?sslmode=disable'
# Migrations are embedded in the binary (`server migrate`), so no extra tool is needed.
DB_URL ?= postgresql://root@localhost:26257/aicontrolplane?sslmode=disable

# Disposable Postgres used ONLY for Bob code generation. bobgen-psql cannot
# introspect CockroachDB (its constraint query uses max(string[]), which CRDB
# rejects), so we generate from a throwaway Postgres with the same portable
# schema. The generated models run against CockroachDB at runtime — validated
# in spike #1. See docs/architecture.md §4.
BOBGEN_PG_PORT := 5544
BOBGEN_PG_DSN  := postgresql://postgres:pg@localhost:$(BOBGEN_PG_PORT)/aicp?sslmode=disable

# Local dev listen address.
LISTEN_ADDR ?= :8899

# Optional secrets-manager wrapper for provider keys, e.g.
#   make run SECRETS_RUN="infisical run --"
# Empty means keys are read from the shell environment.
SECRETS_RUN ?=

.PHONY: help up down build build-linux run test tidy migrate migrate-down migrate-force bobgen usage

help:
	@echo "up / down          start / stop a local single-node CockroachDB (docker compose)"
	@echo "build / build-linux / run / test / tidy"
	@echo "run                relay on $(LISTEN_ADDR), ledger -> DB_URL (keys via $$SECRETS_RUN or env)"
	@echo "usage              tail the newest usage_events rows"
	@echo "migrate            apply migrations to DB_URL (CockroachDB)"
	@echo "migrate-down       roll back one migration"
	@echo "migrate-force V=n  force version (recover from dirty state)"
	@echo "bobgen             regenerate Bob models (spins up a disposable Postgres)"

build:
	go build ./...

# The image is built for linux/amd64, and platform-specific code (SO_REUSEPORT
# in internal/infra/netx) can compile on the dev Mac while failing there — as it
# did once, syscall.SO_REUSEPORT being darwin/BSD-only. Cross-compiling here
# catches that at `make test` speed instead of after a multi-minute Docker
# build. CI runs it; run it before pushing anything that touches netx or build
# tags.
build-linux:
	GOOS=linux GOARCH=amd64 go build ./...

# DB_URL must be passed through: without it the server silently falls back to the
# slog sink and nothing reaches the ledger.
run:
	DB_URL="$(DB_URL)" LISTEN_ADDR=$(LISTEN_ADDR) $(SECRETS_RUN) go run ./cmd/server

# Newest metered calls, for eyeballing while testing a consumer.
usage:
	@psql "$(DB_URL)" -c "SELECT occurred_at, tenant, subject_user, feature, module, provider, model, \
		    input_tokens, cached_input_tokens, output_tokens, cost_usd, cost_known \
		    FROM usage_events ORDER BY occurred_at DESC LIMIT 20;"

test:
	go test ./...

tidy:
	go mod tidy

migrate:
	DB_URL="$(DB_URL)" go run ./cmd/server migrate up

migrate-down:
	DB_URL="$(DB_URL)" go run ./cmd/server migrate down

migrate-force:
	DB_URL="$(DB_URL)" go run ./cmd/server migrate force $(V)

# Stand up a disposable Postgres, apply all (portable) migrations, generate the
# Bob models against it, then tear the container down.
bobgen:
	@docker rm -f aicp-bobgen-pg >/dev/null 2>&1 || true
	docker run -d --name aicp-bobgen-pg -e POSTGRES_PASSWORD=pg -e POSTGRES_DB=aicp -p $(BOBGEN_PG_PORT):5432 postgres:16-alpine >/dev/null
	@until docker exec aicp-bobgen-pg pg_isready -U postgres >/dev/null 2>&1; do sleep 1; done
	migrate -path $(MIGRATE_PATH) -database "$(BOBGEN_PG_DSN)" up
	PSQL_DSN="$(BOBGEN_PG_DSN)" go run github.com/stephenafamo/bob/gen/bobgen-psql@$(BOB_VERSION) -c bobgen.yaml
	@docker rm -f aicp-bobgen-pg >/dev/null 2>&1 || true
	@echo "==> Bob models regenerated. Runtime target remains CockroachDB."

# Local single-node CockroachDB for trying the service end to end.
up:
	docker compose up -d --wait
	docker compose exec cockroachdb ./cockroach sql --insecure -e "CREATE DATABASE IF NOT EXISTS aicontrolplane"

down:
	docker compose down
