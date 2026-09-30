-- Indexes behind the handle-resolution sweep (indexer::get_actors_needing_handle_resolution).
--
-- actor_handle_indexed_at_idx serves the stale non-NULL query
--   WHERE handle IS NOT NULL AND "indexedAt" < $1 ORDER BY "indexedAt" LIMIT n.
-- Without it the planner walks actor_indexed_at_idx from the start and has to
-- step over every NULL-handle row stamped at the epoch (tens of millions on a
-- full-network appview) before reaching the first non-NULL one, on every batch.
--
-- actor_handle_retry_idx serves the per-tries NULL buckets
--   WHERE handle IS NULL AND "handleResolveTries" = $1 AND "indexedAt" < $2.
-- It already exists on deployments that created it by hand alongside the
-- dataplane's add-actor-handle-resolve-tries migration; IF NOT EXISTS keeps
-- this a no-op there.
--
-- CONCURRENTLY: no write lock on actor while building. It cannot run inside a
-- transaction block, so apply with plain `psql -f` (autocommit), not -1.

CREATE INDEX CONCURRENTLY IF NOT EXISTS actor_handle_indexed_at_idx
    ON bsky.actor ("indexedAt")
    WHERE handle IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS actor_handle_retry_idx
    ON bsky.actor ("handleResolveTries", "indexedAt")
    WHERE handle IS NULL;
