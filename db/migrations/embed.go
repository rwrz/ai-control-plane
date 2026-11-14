// Package migrations embeds the SQL migration files so the running binary knows
// which schema it was built against.
//
// This exists because of a failure that is invisible by construction: the relay
// connects to a database, boots cleanly, serves traffic perfectly, and drops
// every usage event because a column the binary writes does not exist yet.
// Nothing upstream notices — metering is off the response path by design, so a
// broken ledger costs no latency and breaks no feature. It ran that way for
// nineteen hours in staging.
//
// Embedding the files gives the boot sequence something to compare against, so
// "the schema is behind this binary" is a fact the service can state at startup
// rather than a conclusion someone has to reach from a log full of SQLSTATE
// 42703.
//
// `server migrate` applies them; the CI pipeline runs it before every
// deploy. They are also the natural source for a boot-time auto-migrate.
// That is deliberately NOT done: the CockroachDB cluster is shared, and a
// migration that fails at boot would turn a metering problem into a service
// that will not start — in the pipeline it just stops the deploy.
package migrations

import (
	"embed"
	"path"
	"strconv"
	"strings"
)

// Files holds every migration, up and down.
//
//go:embed *.sql
var Files embed.FS

// Latest returns the highest migration version compiled into this binary, which
// is the version golang-migrate will have written to schema_migrations once the
// database is fully migrated.
//
// Returns 0 if there are no migrations, which compares harmlessly against any
// real database version rather than reporting a phantom drift.
func Latest() int {
	entries, err := Files.ReadDir(".")
	if err != nil {
		return 0
	}

	latest := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".up.sql") {
			continue
		}
		// golang-migrate names files "<version>_<description>.up.sql".
		version, _, found := strings.Cut(path.Base(name), "_")
		if !found {
			continue
		}
		n, err := strconv.Atoi(version)
		if err != nil {
			continue
		}
		if n > latest {
			latest = n
		}
	}
	return latest
}
