package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// SchemaDriftError reports that the database is not at the version this binary
// expects. It names the DATABASE, which is the fact that actually resolves the
// problem: the chart gives each environment its own database
// (`aicontrolplane`, `aicontrolplane-staging`, …), so the usual mistake is not
// forgetting to migrate but migrating the wrong one — which looks identical
// from the outside, because `make migrate` reports success either way.
type SchemaDriftError struct {
	Database string
	Have     int // applied version, 0 when the database has never been migrated
	Want     int // highest migration compiled into this binary
	Dirty    bool
}

func (e *SchemaDriftError) Error() string {
	if e.Dirty {
		return fmt.Sprintf(
			"database %q is at migration %d and marked DIRTY: a migration failed part-way; "+
				"fix it, then `server migrate force %d` (scripts/migrate-in-cluster.sh in the cluster)", e.Database, e.Have, e.Have)
	}
	if e.Have == 0 {
		return fmt.Sprintf(
			"database %q has never been migrated; this binary expects migration %d",
			e.Database, e.Want)
	}
	return fmt.Sprintf(
		"database %q is at migration %d but this binary expects %d",
		e.Database, e.Have, e.Want)
}

// CheckSchema compares the database's applied migration version against want.
//
// It returns a *SchemaDriftError when they disagree. That is deliberately an
// error the CALLER decides what to do with rather than a fatal one here: an
// out-of-date schema breaks metering, and metering must never take down the
// relay it observes. The point is to say so once, loudly, at boot — instead of
// leaving a per-row SQLSTATE to be noticed by whoever reads the logs next.
//
// A database AHEAD of the binary is not reported. That is the normal state
// during a rolling deploy, when migrations are applied before the new image
// finishes rolling out, and the old binary simply ignores the new columns.
func CheckSchema(ctx context.Context, pool *pgxpool.Pool, want int) error {
	database := pool.Config().ConnConfig.Database

	var (
		have  int
		dirty bool
	)
	err := pool.QueryRow(ctx, `SELECT version, dirty FROM schema_migrations`).Scan(&have, &dirty)
	switch {
	case err == nil:
	case isMissingRelation(err):
		// No schema_migrations at all: the database exists but nothing has ever
		// been applied to it. Reported as version 0 rather than as a query
		// failure, because that is what it means.
		return &SchemaDriftError{Database: database, Have: 0, Want: want}
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	default:
		// An empty table (no rows) also lands here as pgx.ErrNoRows, and means
		// the same thing as a missing one.
		return &SchemaDriftError{Database: database, Have: 0, Want: want}
	}

	if dirty || have < want {
		return &SchemaDriftError{Database: database, Have: have, Want: want, Dirty: dirty}
	}
	return nil
}

// isMissingRelation reports whether err is "relation does not exist" (42P01).
func isMissingRelation(err error) bool {
	var pgErr interface{ SQLState() string }
	return errors.As(err, &pgErr) && pgErr.SQLState() == "42P01"
}
