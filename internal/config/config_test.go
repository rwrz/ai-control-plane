package config

import (
	"net/url"
	"os"
	"testing"
)

func TestResolveDBURLPrefersExplicit(t *testing.T) {
	t.Setenv("DB_URL", "cockroachdb://someone@example:26257/db?sslmode=require")
	t.Setenv("DB_HOST", "should-be-ignored")

	if got, want := resolveDBURL(), "cockroachdb://someone@example:26257/db?sslmode=require"; got != want {
		t.Errorf("resolveDBURL() = %q, want the explicit DB_URL %q", got, want)
	}
}

// Neither DB_URL nor DB_HOST means no ledger — the service must still boot and
// fall back to the slog sink rather than failing.
func TestResolveDBURLEmptyWithoutHost(t *testing.T) {
	t.Setenv("DB_URL", "")
	t.Setenv("DB_HOST", "")

	if got := resolveDBURL(); got != "" {
		t.Errorf("resolveDBURL() = %q, want empty", got)
	}
}

// In-cluster the chart env and the shared secret bundle supply the parts; the
// DSN is composed from them with password auth over TLS.
func TestResolveDBURLComposesFromParts(t *testing.T) {
	t.Setenv("DB_URL", "")
	t.Setenv("DB_HOST", "cockroachdb-public.default.svc.cluster.local")
	t.Setenv("DB_PORT", "26257")
	t.Setenv("DB_USER", "aicp")
	t.Setenv("DB_PASSWORD", "s3cr3t")
	t.Setenv("DB_NAME", "aicpdb")
	t.Setenv("DB_SSLMODE", "")

	got, err := url.Parse(resolveDBURL())
	if err != nil {
		t.Fatalf("composed DSN does not parse: %v", err)
	}

	if got.Scheme != "postgresql" {
		t.Errorf("scheme = %q, want postgresql", got.Scheme)
	}
	if got.Host != "cockroachdb-public.default.svc.cluster.local:26257" {
		t.Errorf("host = %q", got.Host)
	}
	if got.User.Username() != "aicp" {
		t.Errorf("user = %q, want aicp", got.User.Username())
	}
	if pw, set := got.User.Password(); !set || pw != "s3cr3t" {
		t.Errorf("password = %q (set=%v), want s3cr3t", pw, set)
	}
	if got.Path != "/aicpdb" {
		t.Errorf("path = %q, want /aicpdb", got.Path)
	}
	if want := "require"; got.Query().Get("sslmode") != want {
		t.Errorf("sslmode = %q, want %q (TLS on, no cert verification)", got.Query().Get("sslmode"), want)
	}
	// Certificate auth is gone; no cert parameters should be emitted.
	for _, key := range []string{"sslcert", "sslkey", "sslrootcert"} {
		if got.Query().Get(key) != "" {
			t.Errorf("%s should not be set when authenticating by password", key)
		}
	}
}

// A password is free-form and routinely contains characters that are structural
// in a URL. It must be percent-encoded, not concatenated.
func TestResolveDBURLEscapesPassword(t *testing.T) {
	t.Setenv("DB_URL", "")
	t.Setenv("DB_HOST", "crdb")
	t.Setenv("DB_USER", "aicp")
	t.Setenv("DB_PASSWORD", "p@ss:w/rd?x#y")
	t.Setenv("DB_NAME", "aicpdb")

	raw := resolveDBURL()
	got, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("DSN with a special-character password does not parse: %v (%s)", err, raw)
	}
	if pw, _ := got.User.Password(); pw != "p@ss:w/rd?x#y" {
		t.Errorf("password round-trip = %q, want the original", pw)
	}
	if got.Host != "crdb:26257" {
		t.Errorf("host = %q — password characters leaked into the DSN structure", got.Host)
	}
	if got.Path != "/aicpdb" {
		t.Errorf("path = %q — password characters leaked into the DSN structure", got.Path)
	}
}

// No password (e.g. an insecure local cluster) must not emit an empty one.
func TestResolveDBURLOmitsEmptyPassword(t *testing.T) {
	t.Setenv("DB_URL", "")
	t.Setenv("DB_HOST", "crdb")
	t.Setenv("DB_PASSWORD", "")

	got, err := url.Parse(resolveDBURL())
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, set := got.User.Password(); set {
		t.Error("password should be absent, not empty, when DB_PASSWORD is unset")
	}
}

func TestResolveDBURLSSLModeOverridable(t *testing.T) {
	t.Setenv("DB_URL", "")
	t.Setenv("DB_HOST", "crdb")
	t.Setenv("DB_SSLMODE", "verify-full")

	got, _ := url.Parse(resolveDBURL())
	if want := "verify-full"; got.Query().Get("sslmode") != want {
		t.Errorf("sslmode = %q, want %q", got.Query().Get("sslmode"), want)
	}
}

func TestResolveDBURLDefaults(t *testing.T) {
	t.Setenv("DB_URL", "")
	t.Setenv("DB_HOST", "crdb")
	// DB_PORT / DB_USER / DB_NAME intentionally unset.
	for _, k := range []string{"DB_PORT", "DB_USER", "DB_NAME", "DB_PASSWORD"} {
		t.Setenv(k, "")
	}

	got, err := url.Parse(resolveDBURL())
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got.Host != "crdb:26257" {
		t.Errorf("host = %q, want crdb:26257 (default port)", got.Host)
	}
	if got.User.Username() != "aicontrolplane" || got.Path != "/aicontrolplane" {
		t.Errorf("user/db = %q%q, want aicontrolplane", got.User.Username(), got.Path)
	}
}

// Load order is precedence order, because godotenv never overwrites an existing
// variable. A real environment variable must therefore beat both bundles — this
// is what lets the chart's application.env override a mounted secret.
func TestLoadSecretFilesDoesNotClobberRealEnv(t *testing.T) {
	dir := t.TempDir()
	appFile := dir + "/app.env"
	if err := os.WriteFile(appFile, []byte("AICP_TEST_KEY=from_file\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv("AICP_TEST_KEY", "from_env")
	// godotenv.Load is the same call LoadSecretFiles makes; asserting the
	// no-clobber property directly keeps the test independent of /opt paths.
	if err := loadIfPresent(appFile); err != nil {
		t.Fatalf("loadIfPresent: %v", err)
	}
	if got := os.Getenv("AICP_TEST_KEY"); got != "from_env" {
		t.Errorf("AICP_TEST_KEY = %q, want from_env (real env must win)", got)
	}
}

// OpenRouter's key was declared and wired into the relay but never read, so the
// route refused every call in production while its key sat unused in the secret.
func TestLoadReadsEveryProviderKey(t *testing.T) {
	for _, k := range []string{"OPENAI_API_KEY", "OPENROUTER_API_KEY", "GOOGLE_API_KEY",
		"DEEPGRAM_API_KEY", "ELEVENLABS_API_KEY", "INWORLD_API_KEY"} {
		t.Setenv(k, "key-"+k)
	}

	cfg := Load()
	for k, got := range map[string]string{
		"OPENAI_API_KEY":     cfg.OpenAIKey,
		"OPENROUTER_API_KEY": cfg.OpenRouterKey,
		"GOOGLE_API_KEY":     cfg.GoogleKey,
		"DEEPGRAM_API_KEY":   cfg.DeepgramKey,
		"ELEVENLABS_API_KEY": cfg.ElevenLabsKey,
		"INWORLD_API_KEY":    cfg.InworldKey,
	} {
		if got != "key-"+k {
			t.Errorf("%s: got %q, want %q", k, got, "key-"+k)
		}
	}
}
