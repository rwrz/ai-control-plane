package config

import (
	"errors"
	"os"

	"github.com/joho/godotenv"
)

// Secret file locations mounted by the Helm chart. Both are optional —
// locally neither exists and configuration comes from the real environment
// (a secrets manager such as `infisical run`, or a shell export).
const (
	// appSecretsPath is this service's own secret bundle
	// (secret app-secrets-<release>).
	appSecretsPath = "/opt/app-secrets/.env"

	// defaultSecretsPath is the cluster-wide bundle shared by every service
	// (secret named by application.secretName, e.g. "default-production").
	// This is where DB_HOST / DB_PORT come from.
	defaultSecretsPath = "/opt/default-secrets/.env"
)

// LoadSecretFiles loads the chart-mounted .env bundles into the process
// environment. Call it before reading any configuration.
//
// Precedence, most specific first:
//
//  1. real environment variables (chart `application.env`, or a local shell)
//  2. /opt/app-secrets/.env      — this service
//  3. /opt/default-secrets/.env  — cluster-wide defaults
//
// godotenv.Load never overwrites a variable that is already set, so load order
// IS precedence order: whatever is loaded first wins.
//
// NOTE: this is deliberately a copy of a shared loader from the backend's common
// library rather than a dependency on it — this service is standalone and must
// not couple to a consumer. It also fixes an inversion in that original: it
// loads default-secrets FIRST, which (given Load never overwrites) makes the
// cluster-wide defaults silently beat the per-app bundle, the opposite of what
// its own comment intends. Any per-app override of a key that also exists in
// default-secrets is discarded there. Order is reversed here.
func LoadSecretFiles() {
	_ = loadIfPresent(appSecretsPath)     // most specific wins
	_ = loadIfPresent(defaultSecretsPath) // shared fallback
}

// loadIfPresent loads one .env bundle, treating a missing file as success —
// neither mount exists outside the cluster. Variables already set in the
// environment are left untouched (godotenv.Load never overwrites).
func loadIfPresent(path string) error {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return godotenv.Load(path)
}
