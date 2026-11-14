package main

import (
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/cockroachdb"
	"github.com/golang-migrate/migrate/v4/source/iofs"

	"github.com/rwrz/ai-control-plane/db/migrations"
)

const migrateUsage = "usage: server migrate [up | down | version | force <N>]"

// runMigrate applies the embedded migrations to the configured database.
//
// The deploy pipeline runs `/app/server migrate` in a one-off pod that mounts
// the same secrets as the relay, so the database password never leaves the
// cluster. It must exit non-zero on failure: that is what stops the
// pipeline before the Helm deploy ships a binary ahead of its schema.
func runMigrate(dbURL string, args []string, log *slog.Logger) error {
	if dbURL == "" {
		return errors.New("no database configured: set DB_URL, or DB_HOST and friends")
	}
	target, err := migrateURL(dbURL)
	if err != nil {
		return err
	}

	src, err := iofs.New(migrations.Files, ".")
	if err != nil {
		return fmt.Errorf("read embedded migrations: %w", err)
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, target)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer m.Close()

	db := databaseName(target)
	before, dirty, _ := m.Version()
	log.Info("migrate: connected", "database", db, "version", before, "dirty", dirty,
		"binary_expects", migrations.Latest())

	cmd := "up"
	if len(args) > 0 {
		cmd = args[0]
	}
	switch cmd {
	case "up":
		if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			return fmt.Errorf("migrate up on %s: %w", db, err)
		}
	case "down":
		// One step only: a bulk rollback of the ledger schema should never be one
		// mistyped command away.
		if err := m.Steps(-1); err != nil {
			return fmt.Errorf("migrate down on %s: %w", db, err)
		}
	case "version":
		return nil
	case "force":
		if len(args) != 2 {
			return errors.New(migrateUsage)
		}
		v, err := strconv.Atoi(args[1])
		if err != nil {
			return fmt.Errorf("force: %q is not a version", args[1])
		}
		if err := m.Force(v); err != nil {
			return fmt.Errorf("force %d on %s: %w", v, db, err)
		}
	default:
		return errors.New(migrateUsage)
	}

	after, dirty, _ := m.Version()
	log.Info("migrate: done", "database", db, "from", before, "to", after, "dirty", dirty)
	return nil
}

// migrateURL selects golang-migrate's CockroachDB driver. The relay's DSN is
// postgresql:// (pgx), but `make migrate` has always used the cockroachdb driver,
// and its lock table (schema_lock) must stay the one both paths contend on.
func migrateURL(dsn string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("parse database URL: %w", err)
	}
	switch u.Scheme {
	case "postgres", "postgresql", "cockroach", "cockroachdb", "crdb-postgres":
		u.Scheme = "cockroachdb"
	default:
		return "", fmt.Errorf("unsupported database URL scheme %q", u.Scheme)
	}
	return u.String(), nil
}

// databaseName is logged instead of the URL, which carries the password.
func databaseName(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return "?"
	}
	return strings.TrimPrefix(u.Path, "/")
}
