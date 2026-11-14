// Command server runs the ai-control-plane: the AI-aware relay behind Traefik.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	bobpgx "github.com/stephenafamo/bob/drivers/pgx"

	"github.com/rwrz/ai-control-plane/db/migrations"
	"github.com/rwrz/ai-control-plane/internal/config"
	"github.com/rwrz/ai-control-plane/internal/forward"
	"github.com/rwrz/ai-control-plane/internal/infra/db"
	"github.com/rwrz/ai-control-plane/internal/infra/netx"
	"github.com/rwrz/ai-control-plane/internal/ledger"
	"github.com/rwrz/ai-control-plane/internal/pricing"
	"github.com/rwrz/ai-control-plane/internal/relay"
	"github.com/rwrz/ai-control-plane/internal/usage"
)

// slogSink is the Phase-1 usage sink: it logs priced usage. Phase 1b swaps in a
// Bob/CockroachDB ledger writing usage_events.
type slogSink struct{ log *slog.Logger }

func (s slogSink) Record(_ context.Context, ev relay.Event) {
	s.log.Info("usage",
		"tenant", ev.Tenant, "user", ev.User, "feature", ev.Feature, "module", ev.Module,
		"provider", ev.Provider, "model", ev.Model,
		"input", ev.InputTokens, "cached", ev.CachedInputTokens, "output", ev.OutputTokens,
		"cost_usd", ev.CostUSD, "cost_known", ev.CostKnown,
	)
}

// healthHandler reports whether this instance is accepting new work.
//
// Split out of main so it can be tested: the draining answer is the signal a
// deploy script and a load balancer both act on, and it is only ever produced
// during a shutdown that is otherwise awkward to reach from a test.
func healthHandler(draining *atomic.Bool, activeSockets func() int64) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if draining.Load() {
			// 503, not 200: this instance is deliberately no longer a valid
			// target for new work, and reporting healthy would invite exactly
			// the connections the drain exists to avoid. The socket count is
			// included because during a deploy the useful question is not
			// whether it is draining but how much is left.
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = fmt.Fprintf(w, "draining (%d websockets open)\n", activeSockets())
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cfg := config.Load()

	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		if err := runMigrate(cfg.DBURL, os.Args[2:], log); err != nil {
			log.Error("migrate failed", "err", err)
			os.Exit(1)
		}
		return
	}

	prices, err := pricing.Load(cfg.PricingPath)
	if err != nil {
		log.Error("load pricing", "err", err)
		os.Exit(1)
	}

	// Choose the usage sink. Three deployments, one binary:
	//
	//   RELAY_REPORT_URL  sidecar — forward usage to a central control plane
	//   DB_URL            centre  — write usage to CockroachDB, serve the read API
	//   neither           local   — log usage and nothing else
	//
	// Both at once is refused rather than resolved. A sidecar that also owns a
	// database is not a configuration with an obvious winner: picking the
	// ledger silently strands the centre, picking the forwarder silently
	// abandons a database someone deliberately configured. A service that will
	// not start is the cheapest of the three outcomes.
	if cfg.ReportURL != "" && cfg.DBURL != "" {
		log.Error("set either RELAY_REPORT_URL (sidecar) or DB_URL (control plane), not both")
		os.Exit(1)
	}

	var sink relay.Sink = slogSink{log}
	var onShutdown func()
	// The read API needs the relay's keyring, which does not exist yet, so keep
	// the repository and build the handler once the relay is up.
	var usageRepo *usage.Repo
	switch {
	case cfg.ReportURL != "":
		fwd, err := forward.New(forward.Config{
			Endpoint: cfg.ReportURL,
			Key:      cfg.ReportKey,
		}, log)
		if err != nil {
			log.Error("build usage forwarder", "err", err)
			os.Exit(1)
		}
		sink = fwd
		onShutdown = fwd.Close
		// The read API is not mounted: this instance has no ledger to read.
		// Ask the centre instead — it holds every sidecar's rows.
		log.Info("usage sink: forwarding to central control plane",
			"endpoint", cfg.ReportURL)

	case cfg.DBURL != "":
		pool, err := db.NewPool(context.Background(), cfg.DBURL)
		if err != nil {
			log.Error("db connect", "err", err)
			os.Exit(1)
		}
		// Say at boot whether this database can actually accept what the binary
		// writes. A schema behind the binary costs no latency and breaks no
		// feature — it silently drops every usage event, which is exactly why
		// it has to be stated here rather than inferred later from SQLSTATE
		// 42703 in the insert path. Not fatal: metering must never take down
		// the relay it observes.
		if err := db.CheckSchema(context.Background(), pool, migrations.Latest()); err != nil {
			log.Error("SCHEMA BEHIND BINARY — every usage event will be dropped until this is fixed",
				"err", err,
				"fix", "run `server migrate up` against this database with the image being deployed")
		}

		exec := bobpgx.NewPool(pool)
		led := ledger.New(exec, log)
		sink = led
		usageRepo = usage.NewRepo(exec)
		onShutdown = func() { led.Close(); pool.Close() }
		// Naming the database is what makes a mis-targeted migration findable:
		// each environment gets its own (aicontrolplane, aicontrolplane-staging),
		// and migrating the wrong one succeeds just as loudly as the right one.
		log.Info("usage sink: cockroachdb ledger",
			"database", pool.Config().ConnConfig.Database,
			"schema_version", migrations.Latest())

	default:
		log.Info("usage sink: slog (neither DB_URL nor RELAY_REPORT_URL) — /v1/usage/summary disabled")
	}

	rel, err := relay.New(prices, sink, log, relay.Keys{
		OpenAI:     cfg.OpenAIKey,
		OpenRouter: cfg.OpenRouterKey,
		Google:     cfg.GoogleKey,
		Deepgram:   cfg.DeepgramKey,
		ElevenLabs: cfg.ElevenLabsKey,
		Inworld:    cfg.InworldKey,
		AuthKeys:   cfg.AuthKeys,
		AuthMode:   cfg.AuthMode,
	})
	if err != nil {
		log.Error("build relay", "err", err)
		os.Exit(1)
	}

	// draining flips at the first shutdown signal. It is read by /healthz so a
	// load balancer — or a deploy script waiting to signal the previous
	// instance — can tell "up and taking work" from "finishing what it has".
	var draining atomic.Bool

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthHandler(&draining, rel.ActiveWebSockets))
	rel.Register(mux)
	if usageRepo != nil {
		usage.NewHandler(usageRepo, rel.Keyring(), log).Register(mux)
	}

	// Reported usage, for consumers that meter themselves because relaying
	// their traffic would cost more than the measurement is worth — a
	// real-time voice path, where an extra network hop lands on the user.
	usage.NewIngest(sink, prices, rel.Keyring(), log).Register(mux)

	srv := &http.Server{Handler: mux}

	listener, err := netx.Listen(cfg.ListenAddr, cfg.ReusePort)
	if err != nil {
		log.Error("listen", "addr", cfg.ListenAddr, "err", err)
		os.Exit(1)
	}

	// A second, PRIVATE health address, and the reason it has to exist:
	// SO_REUSEPORT means the main port is shared, so a health check against it
	// may be answered by whichever instance the kernel picks. During a deploy
	// that is exactly backwards — the one question worth asking is "is the NEW
	// instance serving yet", and the shared port cannot answer it. This one is
	// bound by a single instance and therefore speaks only for itself.
	var healthSrv *http.Server
	if cfg.HealthAddr != "" {
		healthSrv = &http.Server{Addr: cfg.HealthAddr, Handler: mux}
		go func() {
			log.Info("health listener", "addr", cfg.HealthAddr)
			if err := healthSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				// Fatal on purpose. A deploy script that cannot reach this
				// address concludes the instance is unhealthy and leaves the
				// previous one running — the safe outcome — but only if this
				// instance does not linger pretending to be fine.
				log.Error("health listener failed", "addr", cfg.HealthAddr, "err", err)
				os.Exit(1)
			}
		}()
	}

	go func() {
		log.Info("listening", "addr", cfg.ListenAddr, "reuse_port", cfg.ReusePort)
		if err := srv.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Error("serve", "err", err)
			os.Exit(1)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	// Shutdown, in the order the failure modes demand.
	//
	// 1. Say we are draining, BEFORE closing anything, so a health check
	//    reflects reality from the first moment.
	draining.Store(true)
	log.Info("draining", "grace", cfg.ShutdownGrace,
		"websockets_open", rel.ActiveWebSockets())

	// 2. Stop accepting. Shutdown closes the listener and waits for in-flight
	//    HTTP requests — but NOT for hijacked ones, which every audio socket is.
	//    Its own timeout therefore only bounds ordinary requests.
	shutCtx, cancelShut := context.WithTimeout(context.Background(), 15*time.Second)
	_ = srv.Shutdown(shutCtx)
	cancelShut()
	// The health listener stays up through the drain, deliberately: it is how a
	// deploy script watches this instance wind down, and it answers 503 with
	// the number of sockets still open.

	// 3. Wait for the audio sockets Shutdown ignored. This is the part that
	//    makes a deploy invisible to someone mid-conversation: no new sockets
	//    can arrive now, so this only ever waits on work already in progress.
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), cfg.ShutdownGrace)
	defer cancelDrain()
	if rel.Drain(drainCtx) {
		log.Info("all websockets closed")
	} else {
		// Worth an error: these are live conversations about to be cut, and the
		// count is the evidence for raising SHUTDOWN_GRACE.
		log.Error("shutdown grace expired with websockets still open; cutting them",
			"websockets_open", rel.ActiveWebSockets(), "grace", cfg.ShutdownGrace)
	}

	// 4. Only now flush the usage sink. Sockets that were still streaming were
	//    still producing usage, and closing the ledger or the forwarder first
	//    would discard the last of it.
	if healthSrv != nil {
		_ = healthSrv.Close()
	}
	if onShutdown != nil {
		onShutdown()
	}
	log.Info("stopped")
}
