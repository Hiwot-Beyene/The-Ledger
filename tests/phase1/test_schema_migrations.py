"""Static checks: migrations match Phase 1 rubric and stay aligned with src/schema.sql."""

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
UP = ROOT / "src/migrations/001_event_store.up.sql"
DOWN = ROOT / "src/migrations/001_event_store.down.sql"
SCHEMA = ROOT / "src/schema.sql"


def test_forward_migration_exists_and_defines_four_tables():
    text = UP.read_text()
    assert "CREATE EXTENSION IF NOT EXISTS pgcrypto" in text
    for name in ("events", "event_streams", "projection_checkpoints", "outbox"):
        assert f"CREATE TABLE IF NOT EXISTS {name}" in text
    assert "CONSTRAINT uq_stream_position UNIQUE (stream_id, stream_position)" in text
    assert "GENERATED ALWAYS AS IDENTITY" in text
    assert "REFERENCES events(event_id)" in text
    for idx in (
        "idx_events_stream_id",
        "idx_events_global_pos",
        "idx_events_type",
        "idx_events_recorded",
        "idx_events_recorded_brin",
    ):
        assert f"CREATE INDEX IF NOT EXISTS {idx}" in text
    assert "idx_outbox_unpublished_created" in text
    assert "CREATE TABLE IF NOT EXISTS snapshots" in text
    assert "idx_snapshots_stream_latest" in text
    assert "archived_at" in text
    assert "published_at" in text
    assert "attempts" in text


def test_rollback_drops_dependents_before_events_and_indexes_first():
    text = DOWN.read_text()
    lines = [ln.strip() for ln in text.splitlines() if ln.strip() and not ln.strip().startswith("--")]
    drop_table_at = next(i for i, ln in enumerate(lines) if ln.startswith("DROP TABLE"))
    assert all(lines[i].startswith("DROP INDEX") for i in range(drop_table_at)), (
        "all DROP INDEX must precede first DROP TABLE to preserve clean teardown"
    )
    assert text.index("DROP TABLE IF EXISTS outbox") < text.index("DROP TABLE IF EXISTS events")


def test_schema_sql_parity_with_forward_migration():
    """Core DDL fragments must appear in both annotated schema and migration up script."""
    up = UP.read_text()
    schema = SCHEMA.read_text()
    fragments = (
        "CONSTRAINT uq_stream_position UNIQUE (stream_id, stream_position)",
        "REFERENCES events(event_id)",
        "CREATE INDEX IF NOT EXISTS idx_events_stream_id",
        "CREATE INDEX IF NOT EXISTS idx_events_global_pos",
        "CREATE INDEX IF NOT EXISTS idx_events_type",
        "CREATE INDEX IF NOT EXISTS idx_events_recorded",
        "CREATE INDEX IF NOT EXISTS idx_events_recorded_brin",
        "CREATE INDEX IF NOT EXISTS idx_outbox_unpublished_created",
        "CREATE TABLE IF NOT EXISTS snapshots",
        "CREATE INDEX IF NOT EXISTS idx_snapshots_stream_latest",
    )
    for f in fragments:
        assert f in up, f"missing in 001_event_store.up.sql: {f!r}"
        assert f in schema, f"missing in schema.sql: {f!r}"
