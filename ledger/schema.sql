-- Canonical DDL + maintainer comments. Forward/rollback scripts: ledger/migrations/README.md
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- events: append-only log. Read patterns: (1) replay by stream, (2) global order for projectors,
-- (3) filter by type/time for audit and ops.
CREATE TABLE IF NOT EXISTS events (
  -- PK: stable row identity; referenced by outbox and used for idempotent consumers.
  event_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  stream_id        TEXT NOT NULL,
  -- Monotonic per stream; paired with stream_id for ordered replay and OCC append.
  stream_position  BIGINT NOT NULL,
  -- Monotonic across all streams; single cursor for catch-up and checkpointing.
  global_position  BIGINT GENERATED ALWAYS AS IDENTITY,
  event_type       TEXT NOT NULL,
  event_version    SMALLINT NOT NULL DEFAULT 1,
  payload          JSONB NOT NULL,
  metadata         JSONB NOT NULL DEFAULT '{}'::jsonb,
  recorded_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  -- CONSTRAINT uq_stream_position: one position per stream — prevents double-append and gaps in
  -- optimistic concurrency; append path must allocate the next position atomically.
  CONSTRAINT uq_stream_position UNIQUE (stream_id, stream_position)
);

-- idx_events_stream_id: range scans WHERE stream_id = ? ORDER BY stream_position — aggregate load,
-- stream tail reads, and append validation against latest position.
CREATE INDEX IF NOT EXISTS idx_events_stream_id ON events (stream_id, stream_position);
-- idx_events_global_pos: range scans WHERE global_position > ? ORDER BY global_position — projector
-- and subscriber replay from last checkpoint without full table scan ordering.
CREATE INDEX IF NOT EXISTS idx_events_global_pos ON events (global_position);
-- idx_events_type: equality filter on event_type — targeted backfills, audit slices, typed projections.
CREATE INDEX IF NOT EXISTS idx_events_type ON events (event_type);
-- idx_events_recorded: range/filter on recorded_at — ops dashboards, retention windows, incident windows.
CREATE INDEX IF NOT EXISTS idx_events_recorded ON events (recorded_at);

-- event_streams: registry row per stream; current_version drives OCC; archived_at blocks new appends.
CREATE TABLE IF NOT EXISTS event_streams (
  -- PK stream_id: natural key matches event stream_id prefix convention (e.g. loan-{id}).
  stream_id        TEXT PRIMARY KEY,
  aggregate_type   TEXT NOT NULL,
  current_version  BIGINT NOT NULL DEFAULT 0,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  archived_at      TIMESTAMPTZ,
  metadata         JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- projection_checkpoints: one row per projector name; PK projection_name enforces single cursor per consumer.
CREATE TABLE IF NOT EXISTS projection_checkpoints (
  projection_name  TEXT PRIMARY KEY,
  last_position    BIGINT NOT NULL DEFAULT 0,
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- outbox: transactional enqueue with events; worker polls unpublished rows.
CREATE TABLE IF NOT EXISTS outbox (
  -- PK id: unique outbox message row (separate from event_id — one event may fan out later if extended).
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  -- FK events(event_id): every outbox row ties to a persisted event — referential integrity and traceability.
  event_id         UUID NOT NULL REFERENCES events(event_id),
  destination      TEXT NOT NULL,
  payload          JSONB NOT NULL,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  published_at     TIMESTAMPTZ,
  attempts         SMALLINT NOT NULL DEFAULT 0
);
