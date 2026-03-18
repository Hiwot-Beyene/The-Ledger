# Phase 1 Event Store Design

## Rubric alignment

| Criterion | How the schema satisfies it |
|-----------|----------------------------|
| **Table completeness** | All four tables: `events`, `event_streams`, `projection_checkpoints`, `outbox`. |
| **Structural correctness** | `global_position` is `GENERATED ALWAYS AS IDENTITY` for total ordering; `uq_stream_position` enforces one row per `(stream_id, stream_position)`; `outbox.event_id` references `events(event_id)` for guaranteed linkage to persisted facts. |
| **Indexing strategy** | Four indexes on `events` map to: stream replay `(stream_id, stream_position)`, global catch-up `(global_position)`, type-filtered reads `(event_type)`, time-window ops `(recorded_at)`. |
| **Lifecycle support** | `event_streams.archived_at` freezes streams; `outbox.published_at` and `outbox.attempts` support delivery tracking and retries. |

## EventStore implementation (`ledger/event_store.py`)

| Rubric | Implementation |
|--------|----------------|
| **Transactional append** | Single `asyncpg` transaction: CAS `UPDATE event_streams` → `INSERT events` → `INSERT outbox` per event. |
| **Optimistic concurrency** | `UPDATE … WHERE current_version = $expected`; `OptimisticConcurrencyError` if no row updated (never retried). |
| **`load_all` batching** | Long-lived connection; pages with `LIMIT batch_size`; cursor via `global_position`; `batch_size >= 1` enforced. |
| **Transient DB retry** | `_run_with_db_retry`: exponential backoff (`db_retry_base_delay * 2**attempt`, cap `db_retry_max_delay`); `is_transient_db_error` excludes OCC. |
| **Tests** | No DB: `tests/test_event_store_retry.py`, `tests/phase1/test_event_store.py`. PostgreSQL: `tests/test_event_store.py` (`pytest -m postgres_integration`), `docs/local_postgres_testing.md`. |

## Command handler pattern (`ledger/domain/commands/`)

| Rubric | Implementation |
|--------|------------------|
| **Orchestration** | Each `handle_*`: `load` loan and/or agent/compliance streams → guard methods → `store_dict_*` factories → single `append`. |
| **OCC + trace** | `expected_version` = replayed aggregate `version`; `correlation_id` / `causation_id` passed through to `append`. |
| **Observability** | `run_with_command_observability` (`observability.py`): `handler_start` / `handler_ok` (`duration_ms`) / `handler_failed`. Logger: **`ledger.commands`**. |
| **Tests** | `tests/test_command_handlers.py` (`pytest -m command_handler`). Details: `ledger/domain/commands/README.md`. |

## Migrations and rollback

- **Forward:** `ledger/migrations/001_event_store.up.sql` — must match `ledger/schema.sql` DDL (contract for code and reviewers).
- **Rollback:** `ledger/migrations/001_event_store.down.sql` — drops `outbox` before `events` so the FK is respected; drops event indexes before tables.
- **Intent comments** for columns, constraints, and indexes live in `ledger/schema.sql` for maintainers; `DESIGN.md` summarizes justification.
- **CI:** `tests/test_schema_migrations.py` checks that migration files contain the required objects and safe drop order.

## Schema Column Justification

### `events`
- `event_id`: immutable primary key for audit references and outbox linkage.
- `stream_id`: aggregate stream key (`loan-...`, `credit-...`) for replay boundaries.
- `stream_position`: per-stream ordering and optimistic concurrency integrity.
- `global_position`: monotonic global replay cursor for projections/daemons.
- `event_type`: dispatcher key (`_on_<event_type>`) and filtering.
- `event_version`: supports read-time upcasting as contracts evolve.
- `payload`: domain fact body; immutable business data.
- `metadata`: causal tracing (`correlation_id`, `causation_id`) and operational tags.
- `recorded_at`: temporal audit and time-sliced queries.
- `uq_stream_position(stream_id, stream_position)`: enforces single-writer outcome at DB level.

Indexes (constraint vs index):
- **`uq_stream_position` (UNIQUE constraint):** integrity — no duplicate positions per stream; backs OCC append allocation.
- **`idx_events_stream_id`:** read pattern — aggregate replay / tail by `stream_id` ordered by `stream_position`.
- **`idx_events_global_pos`:** read pattern — all-stream ordered scan from `global_position` for projectors.
- **`idx_events_type`:** read pattern — filter by `event_type` (audit, selective replay).
- **`idx_events_recorded`:** read pattern — filter or range on `recorded_at` (ops, retention windows).

### `event_streams`
- `stream_id`: stream identity and lookup key.
- `aggregate_type`: partitioning/ops visibility by aggregate family.
- `current_version`: CAS target for optimistic concurrency checks.
- `created_at`: stream lifecycle origin.
- `archived_at`: lifecycle freeze marker; prevents further writes when archived.
- `metadata`: stream-level labels/ownership/context.

### `projection_checkpoints`
- `projection_name`: unique projection consumer identity.
- `last_position`: replay checkpoint for crash-safe resume.
- `updated_at`: staleness and lag monitoring support.

### `outbox`
- `id`: independent outbox identity (supports fanout rows per event).
- `event_id`: hard link to canonical event.
- `destination`: target publisher/channel routing.
- `payload`: serialized publish envelope.
- `created_at`: retry order and SLA tracking.
- `published_at`: delivered marker.
- `attempts`: retry/dead-letter policy support.

## Missing Elements Worth Adding (Future Hardening)

- `NOT NULL` and FK on `events.stream_id -> event_streams.stream_id` (requires migration strategy for bootstrap loads).
- `CHECK (stream_position > 0)` and `CHECK (current_version >= 0)` to lock in 1-based stream positions.
- `outbox` indexes: `(published_at, created_at)` and `(event_id, destination)` for publisher efficiency and idempotency.
- Trigger for `projection_checkpoints.updated_at` on update to avoid stale timestamps.
- Optional `idempotency_key` in `events.metadata` (or dedicated column) for exactly-once command dedupe.
- Optional archival policy metadata (`archived_reason`, `archived_by`) for regulated audits.

## Related

- **Aggregate design & state reconstruction:** `ledger/domain/aggregates/README.md` (invariants, Mermaid state diagrams, guard tests under `tests/test_*_aggregate.py`).
- **Command handlers & observability:** **Command handler pattern** table above; `ledger/domain/commands/README.md`; `pytest -m command_handler`.
- **Postgres EventStore integration tests:** `docs/local_postgres_testing.md` (why tests skip without DB, Docker URL, `pytest -m postgres_integration`).
- **Domain events & factories:** `ledger/schema/README.md` (`events.py`, `event_factories.py`, `tests/test_domain_event_models.py`).
- **EventStore code & tests:** `ledger/event_store.py` and the **EventStore implementation** table above.
