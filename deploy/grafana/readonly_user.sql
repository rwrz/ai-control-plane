-- Read-only CockroachDB user for Grafana.
--
-- Grafana queries usage_events directly (see README.md for why it bypasses
-- /v1/usage/summary), so it gets a role that can do nothing but read that one
-- table. A dashboard must never be able to mutate the ledger — usage_events is
-- an append-only record of money spent, and the writer is the relay alone.
--
-- Run as an admin against the aicontrolplane database. Replace the password:
--   cockroach sql --url "$ADMIN_DB_URL" < deploy/grafana/readonly_user.sql

CREATE USER IF NOT EXISTS grafana_ro WITH PASSWORD 'CHANGE_ME';

GRANT CONNECT ON DATABASE aicontrolplane TO grafana_ro;
GRANT SELECT ON TABLE usage_events TO grafana_ro;

-- Explicitly NOT granted: INSERT/UPDATE/DELETE on usage_events, any access to
-- schema_migrations, and any future table. New tables must be granted
-- deliberately rather than inherited, so a later migration cannot silently widen
-- Grafana's reach.

-- Every Grafana session reads slightly-stale data, and that is deliberate.
--
-- usage_events has exactly one writer — the relay — appending continuously
-- (~50k rows/day). The dashboard runs 22 multi-second aggregate scans over that
-- same table on every load. A read at the current timestamp is not free against
-- a live write path: it bumps the timestamp cache, which can push a concurrent
-- writer's commit timestamp forward and force it to retry, and it has to resolve
-- any write intents it runs into. None of that blocks in the lock sense, but it
-- is contention the relay does not need to carry so somebody can look at a
-- chart.
--
-- A historical read has neither problem. It reads a settled MVCC snapshot, so it
-- cannot conflict with an in-flight write, and once the timestamp is past the
-- closed timestamp (~4.8s) it can be served by any follower replica instead of
-- the leaseholder — which also takes the scan load off the range that is being
-- written to.
--
-- Set on the ROLE rather than in the panel SQL because it cannot be done in the
-- panel SQL. `AS OF SYSTEM TIME` is only accepted in the FROM clause of a
-- top-level SELECT: it parses on a subquery but is a syntax error on a query
-- with a CTE, and "Where a session's money goes" is a CTE. A role default
-- applies to every statement on every connection Grafana opens, uniformly, with
-- no way for a new panel to forget it.
--
-- The cost is that the dashboard is ~5s behind. For a cost ledger read in hourly
-- and daily buckets, that is not a cost at all.
--
-- Requires CREATEROLE or MODIFYCLUSTERSETTING to run, so it belongs with the
-- admin-run statements above rather than anywhere the relay could reach.
ALTER ROLE grafana_ro SET default_transaction_use_follower_reads = true;
