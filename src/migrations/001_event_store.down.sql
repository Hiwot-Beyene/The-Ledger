-- Rollback Phase 1 event store (DESTRUCTIVE — all ledger data in these tables is removed).
-- Order preserves referential integrity: outbox references events(event_id), so outbox first.

DROP INDEX IF EXISTS idx_events_recorded;
DROP INDEX IF EXISTS idx_events_recorded_brin;
DROP INDEX IF EXISTS idx_events_type;
DROP INDEX IF EXISTS idx_events_global_pos;
DROP INDEX IF EXISTS idx_events_stream_id;
DROP INDEX IF EXISTS idx_streams_aggregate_archived;
DROP INDEX IF EXISTS idx_outbox_unpublished_created;
DROP INDEX IF EXISTS idx_snapshots_stream_latest;
DROP INDEX IF EXISTS idx_compliance_snapshot_lookup;
DROP INDEX IF EXISTS idx_compliance_snapshot_lookup_bg;
DROP INDEX IF EXISTS idx_app_summary_last_event_at;

DROP TABLE IF EXISTS compliance_audit_snapshots_bg;
DROP TABLE IF EXISTS compliance_audit_current_bg;
DROP TABLE IF EXISTS compliance_audit_snapshots;
DROP TABLE IF EXISTS compliance_audit_current;
DROP TABLE IF EXISTS application_decision_attribution;
DROP TABLE IF EXISTS agent_session_index;
DROP TABLE IF EXISTS agent_performance_ledger;
DROP TABLE IF EXISTS application_summary;
DROP TABLE IF EXISTS snapshots;
DROP TABLE IF EXISTS outbox;
DROP TABLE IF EXISTS projection_checkpoints;
DROP TABLE IF EXISTS event_streams;
DROP TABLE IF EXISTS events;
