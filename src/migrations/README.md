# Event store migrations

| File | Purpose |
|------|---------|
| `001_event_store.up.sql` | Same DDL as `src/schema.sql`, with short comments on constraint, indexes, and FK intent. |
| `001_event_store.down.sql` | Ordered drops for dev rollback (respects FK from `outbox` to `events`). |

Apply from repo root:

```bash
export DATABASE_URL=postgresql://…
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f src/migrations/001_event_store.up.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f src/migrations/001_event_store.down.sql
```

`src/schema.sql` is the annotated reference; keep it and `001_event_store.up.sql` aligned. `tests/phase1/test_schema_migrations.py` asserts parity on key DDL tokens.
