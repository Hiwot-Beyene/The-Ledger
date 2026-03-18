# Ledger migrations

## Files

| File | Purpose |
|------|---------|
| `001_event_store.up.sql` | Same DDL as `ledger/schema.sql`, with short comments on constraint, indexes, and FK intent. |
| `001_event_store.down.sql` | Drop in safe order (indexes → `outbox` → other tables → `events`). |

## Apply / rollback

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f ledger/migrations/001_event_store.up.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f ledger/migrations/001_event_store.down.sql
```

## Guarantees preserved by up/down

- **Up** recreates: global ordering (`global_position` IDENTITY), `uq_stream_position`, FK `outbox.event_id → events.event_id`, `archived_at` / `published_at` / `attempts` lifecycle columns, and the four event indexes aligned to read patterns.
- **Down** removes dependents before `events` so FK and index drops do not leave orphans.

`ledger/schema.sql` is the annotated reference; keep it and `001_event_store.up.sql` aligned. `tests/test_schema_migrations.py` asserts parity on key DDL tokens.
