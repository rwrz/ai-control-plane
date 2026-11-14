// Package config loads service configuration from the environment. In the
// cluster the values arrive as .env bundles mounted by the Helm chart (see
// secrets.go); locally they come from a secrets manager or the shell.
package config

import (
	"net"
	"net/url"
	"os"
	"time"
)

// Config is the resolved service configuration.
type Config struct {
	ListenAddr  string
	PricingPath string
	OpenAIKey   string
	GoogleKey   string
	// OpenRouterKey enables the openrouter route. OpenRouter fronts many
	// vendors on one credential, which is what makes it useful for evaluating
	// candidate models without an account per vendor.
	OpenRouterKey string
	// Audio providers. Empty means that provider is unconfigured and the relay
	// refuses requests to it rather than forwarding an empty credential.
	DeepgramKey   string
	ElevenLabsKey string
	InworldKey    string
	// AuthKeys is the caller keyring, "key:tenant,key:tenant". Empty leaves the
	// relay unauthenticated, which is only safe while it is cluster-internal.
	AuthKeys string
	// AuthMode is "enforce" (default) or "shadow". Shadow serves callers with
	// no valid key while logging them, so keys can be rolled out to consumers
	// without the relay failing their calls in the window between.
	AuthMode string
	DBURL    string // CockroachDB DSN; empty disables the ledger and the read API

	// ReportURL turns this instance into a SIDECAR: instead of writing usage to
	// its own database it forwards it to the control plane at this address.
	// That is how a relay runs next to a latency-sensitive consumer — the
	// consumer's hop becomes loopback, and only the priced summary crosses the
	// network. Mutually exclusive with DBURL.
	ReportURL string
	// ReportKey authenticates to that control plane. It also decides the tenant
	// every forwarded row is booked against, since the receiver takes the
	// tenant from the key and never from the body.
	ReportKey string

	// ShutdownGrace bounds how long shutdown waits for live WebSocket
	// conversations to end. Audio sockets last as long as the session does, so
	// on a voice sidecar this is the difference between a deploy nobody
	// notices and one that cuts off every call in progress.
	ShutdownGrace time.Duration
	// ReusePort enables SO_REUSEPORT, letting a replacement instance bind the
	// same port while this one drains. See internal/infra/netx.
	ReusePort bool
	// HealthAddr is an optional PRIVATE address serving the same routes. With
	// ReusePort the main port is shared between instances, so only an address
	// bound by exactly one of them can answer "is THIS instance serving?" —
	// the question a deploy has to answer before retiring its predecessor.
	HealthAddr string
}

// Load reads configuration, first pulling in any chart-mounted secret files.
func Load() Config {
	LoadSecretFiles()

	return Config{
		ListenAddr:    env("LISTEN_ADDR", ":8080"),
		PricingPath:   env("PRICING_PATH", "pricing.toml"),
		OpenAIKey:     os.Getenv("OPENAI_API_KEY"),
		OpenRouterKey: os.Getenv("OPENROUTER_API_KEY"),
		GoogleKey:     os.Getenv("GOOGLE_API_KEY"),
		DeepgramKey:   os.Getenv("DEEPGRAM_API_KEY"),
		ElevenLabsKey: os.Getenv("ELEVENLABS_API_KEY"),
		InworldKey:    os.Getenv("INWORLD_API_KEY"),
		AuthKeys:      os.Getenv("RELAY_AUTH_KEYS"),
		AuthMode:      os.Getenv("RELAY_AUTH_MODE"),
		DBURL:         resolveDBURL(),
		ReportURL:     os.Getenv("RELAY_REPORT_URL"),
		ReportKey:     os.Getenv("RELAY_REPORT_KEY"),
		ShutdownGrace: envDuration("SHUTDOWN_GRACE", 30*time.Second),
		ReusePort:     os.Getenv("LISTEN_REUSE_PORT") == "true",
		HealthAddr:    os.Getenv("HEALTH_ADDR"),
	}
}

// resolveDBURL prefers an explicit DB_URL — how local dev is configured, and the
// escape hatch for any DSN this composition cannot express. Failing that it
// builds one from the parts the secret bundle and chart env supply:
//
//	DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
//
// Auth is by password over TLS. sslmode defaults to `require`, which encrypts
// the connection without verifying the server certificate — appropriate for an
// in-cluster hop to CockroachDB. Override with DB_SSLMODE (e.g. `verify-full`)
// if a CA is ever mounted.
//
// Returns "" when DB_HOST is absent, which leaves the ledger on its slog
// fallback rather than failing to boot.
func resolveDBURL() string {
	if explicit := os.Getenv("DB_URL"); explicit != "" {
		return explicit
	}

	host := os.Getenv("DB_HOST")
	if host == "" {
		return ""
	}

	q := url.Values{}
	q.Set("sslmode", env("DB_SSLMODE", "require"))

	dsn := url.URL{
		Scheme: "postgresql",
		// url.UserPassword percent-encodes both parts, so a password containing
		// @ : / or ? cannot corrupt the DSN.
		User:     credentials(env("DB_USER", "aicontrolplane"), os.Getenv("DB_PASSWORD")),
		Host:     net.JoinHostPort(host, env("DB_PORT", "26257")),
		Path:     "/" + env("DB_NAME", "aicontrolplane"),
		RawQuery: q.Encode(),
	}
	return dsn.String()
}

// credentials builds the userinfo section, omitting the password entirely when
// none is set rather than emitting an empty one.
func credentials(user, password string) *url.Userinfo {
	if password == "" {
		return url.User(user)
	}
	return url.UserPassword(user, password)
}

// envDuration reads a Go duration ("10m", "30s"). An unparseable value falls
// back to the default rather than failing to boot: shutdown behaviour is not
// worth refusing to start over, and the value is logged where it is used.
func envDuration(k string, def time.Duration) time.Duration {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		return def
	}
	return d
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
