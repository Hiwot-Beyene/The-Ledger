-- Rollback Phase 1 event store (DESTRUCTIVE — all ledger data in these tables is removed).
-- Order preserves referential integrity: outbox references events(event_id), so outbox first.

DROP INDEX IF EXISTS idx_events_recorded;
DROP INDEX IF EXISTS idx_events_type;
DROP INDEX IF EXISTS idx_events_global_pos;
DROP INDEX IF EXISTS idx_events_stream_id;

DROP TABLE IF EXISTS outbox;
DROP TABLE IF EXISTS projection_checkpoints;
DROP TABLE IF EXISTS event_streams;
DROP TABLE IF EXISTS events;
