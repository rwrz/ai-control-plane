// Package db builds the pgx connection pool used against CockroachDB.
package db

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPool opens and pings a pgx pool. It accepts cockroachdb://, cockroach://,
// postgres://, or postgresql:// DSNs — the cockroachdb schemes (used by
// golang-migrate) are rewritten to postgres:// for pgx, which speaks the same
// wire protocol to CockroachDB.
func NewPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, normalizeScheme(dsn))
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

func normalizeScheme(dsn string) string {
	for _, p := range []string{"cockroachdb://", "cockroach://"} {
		if strings.HasPrefix(dsn, p) {
			return "postgres://" + strings.TrimPrefix(dsn, p)
		}
	}
	return dsn
}
