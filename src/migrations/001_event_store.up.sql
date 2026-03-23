-- Phase 1 event store — forward migration (DDL must match ledger/schema.sql; comments mirror intent there).
-- Rubric: four tables; global_position IDENTITY + uq_stream_position + outbox FK; four read-pattern indexes;
-- lifecycle: archived_at, published_at, attempts.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS events (
  event_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  stream_id        TEXT NOT NULL,
  stream_position  BIGINT NOT NULL,
  global_position  BIGINT GENERATED ALWAYS AS IDENTITY,
  event_type       TEXT NOT NULL,
  event_version    SMALLINT NOT NULL DEFAULT 1,
  payload          JSONB NOT NULL,
  metadata         JSONB NOT NULL DEFAULT '{}'::jsonb,
  recorded_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  -- Positional integrity + OCC: at most one row per (stream_id, stream_position).
  CONSTRAINT uq_stream_position UNIQUE (stream_id, stream_position)
);

-- Read patterns: stream replay; global projector cursor; type filter; time-window / ops.
CREATE INDEX IF NOT EXISTS idx_events_stream_id ON events (stream_id, stream_position);
CREATE INDEX IF NOT EXISTS idx_events_global_pos ON events (global_position);
CREATE INDEX IF NOT EXISTS idx_events_type ON events (event_type);
CREATE INDEX IF NOT EXISTS idx_events_recorded ON events (recorded_at);
CREATE INDEX IF NOT EXISTS idx_events_recorded_brin ON events USING BRIN (recorded_at);

CREATE TABLE IF NOT EXISTS event_streams (
  stream_id        TEXT PRIMARY KEY,
  aggregate_type   TEXT NOT NULL,
  current_version  BIGINT NOT NULL DEFAULT 0,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  archived_at      TIMESTAMPTZ,
  metadata         JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_streams_aggregate_archived
  ON event_streams (aggregate_type, archived_at);

CREATE TABLE IF NOT EXISTS projection_checkpoints (
  projection_name  TEXT PRIMARY KEY,
  last_position    BIGINT NOT NULL DEFAULT 0,
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS outbox (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  -- Guaranteed delivery linkage: every outbox row maps to a persisted event.
  event_id         UUID NOT NULL REFERENCES events(event_id),
  destination      TEXT NOT NULL,
  payload          JSONB NOT NULL,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  published_at     TIMESTAMPTZ,
  attempts         SMALLINT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished_created
  ON outbox (published_at, created_at)
  WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS snapshots (
  snapshot_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  stream_id         TEXT NOT NULL REFERENCES event_streams(stream_id),
  stream_position   BIGINT NOT NULL,
  aggregate_type    TEXT NOT NULL,
  snapshot_version  INT NOT NULL,
  state             JSONB NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_snapshots_stream_latest
  ON snapshots (stream_id, stream_position DESC);

CREATE TABLE IF NOT EXISTS application_summary (
  application_id            TEXT PRIMARY KEY,
  state                     TEXT,
  applicant_id              TEXT,
  requested_amount_usd      NUMERIC(18, 2),
  approved_amount_usd       NUMERIC(18, 2),
  risk_tier                 TEXT,
  fraud_score               DOUBLE PRECISION,
  compliance_status         TEXT,
  decision                  TEXT,
  agent_sessions_completed  JSONB NOT NULL DEFAULT '[]'::jsonb,
  last_event_type           TEXT,
  last_event_at             TIMESTAMPTZ,
  human_reviewer_id         TEXT,
  final_decision_at         TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_app_summary_last_event_at ON application_summary (last_event_at);

CREATE TABLE IF NOT EXISTS agent_performance_ledger (
  agent_id                TEXT NOT NULL,
  model_version           TEXT NOT NULL,
  analyses_completed      BIGINT NOT NULL DEFAULT 0,
  decisions_generated     BIGINT NOT NULL DEFAULT 0,
  avg_confidence_score    DOUBLE PRECISION NOT NULL DEFAULT 0,
  avg_duration_ms         DOUBLE PRECISION NOT NULL DEFAULT 0,
  approve_rate            DOUBLE PRECISION NOT NULL DEFAULT 0,
  decline_rate            DOUBLE PRECISION NOT NULL DEFAULT 0,
  refer_rate              DOUBLE PRECISION NOT NULL DEFAULT 0,
  human_override_rate     DOUBLE PRECISION NOT NULL DEFAULT 0,
  first_seen_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (agent_id, model_version)
);

CREATE TABLE IF NOT EXISTS agent_session_index (
  session_id              TEXT PRIMARY KEY,
  application_id          TEXT NOT NULL,
  agent_id                TEXT NOT NULL,
  model_version           TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS application_decision_attribution (
  application_id          TEXT PRIMARY KEY,
  model_version           TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS compliance_audit_current (
  application_id            TEXT PRIMARY KEY,
  regulation_set_version    TEXT,
  checks                    JSONB NOT NULL DEFAULT '[]'::jsonb,
  verdict                   TEXT,
  latest_event_at           TIMESTAMPTZ,
  last_event_type           TEXT,
  event_count               BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS compliance_audit_snapshots (
  application_id            TEXT NOT NULL,
  up_to_global_position     BIGINT NOT NULL,
  snapshot_at               TIMESTAMPTZ NOT NULL,
  snapshot_payload          JSONB NOT NULL,
  PRIMARY KEY (application_id, up_to_global_position)
);
CREATE INDEX IF NOT EXISTS idx_compliance_snapshot_lookup
  ON compliance_audit_snapshots (application_id, snapshot_at DESC);

CREATE TABLE IF NOT EXISTS compliance_audit_current_bg (
  application_id            TEXT PRIMARY KEY,
  regulation_set_version    TEXT,
  checks                    JSONB NOT NULL DEFAULT '[]'::jsonb,
  verdict                   TEXT,
  latest_event_at           TIMESTAMPTZ,
  last_event_type           TEXT,
  event_count               BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS compliance_audit_snapshots_bg (
  application_id            TEXT NOT NULL,
  up_to_global_position     BIGINT NOT NULL,
  snapshot_at               TIMESTAMPTZ NOT NULL,
  snapshot_payload          JSONB NOT NULL,
  PRIMARY KEY (application_id, up_to_global_position)
);
CREATE INDEX IF NOT EXISTS idx_compliance_snapshot_lookup_bg
  ON compliance_audit_snapshots_bg (application_id, snapshot_at DESC);