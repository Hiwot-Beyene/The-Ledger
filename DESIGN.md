# Ledger design — implementation & Phase 1 schema

This document records **what was built**, **explicit tradeoffs**, **quantitative reliability budgets**, and **reflection** on mistakes uncovered during implementation. The tables below the divider retain Phase 1 rubric-level detail (DDL, indexes, migrations).

## Architectural reasoning

### 1. Event-sourced write model

The system is built around an append-only **`events`** log with **`stream_id` + `stream_position`** (per-aggregate ordering) and **`global_position`** (monotonic all-stream cursor). **`event_streams.current_version`** backs optimistic concurrency on append. The **outbox** rows are inserted in the **same transaction** as new events so publish-side consumers see at-least-once delivery with a durable handoff.

**Tradeoffs:** Per-stream writers scale mentally and operationally, but clients must treat **`OptimisticConcurrencyError`** as a normal retry path. Global ordering for projections trades strict “every stream equally fresh” for a simple single cursor; hot streams can advance ahead of cold ones without tearing the log.

### 2. Projections and CQRS

Three materialized consumers (**`application_summary`**, **`agent_performance_ledger`**, **`compliance_audit_view`**) project from `events`. The daemon pages **`load_all`** from the **minimum** checkpoint across registered projections, applies handlers under bounded retries, then advances checkpoints. **`ProjectionDaemon.get_health`** and MCP **`resource_health`** expose **`max_projection_lag`** and **`slo_projection_catchup_ok`** vs **`LEDGER_PROJECTION_MAX_LAG_EVENTS`** (default **100000** until exceeded).

**Tradeoffs:** **Min-checkpoint batching** means one slow projection back-pressures the shared read cursor—operational visibility per projection (lags map) is the mitigation, not independent cursors. After **`max_retries`**, advancing past a poison-pill event favors **liveness** over halting the line for one bad payload; handlers should be idempotent.

### 3. Domain commands and aggregates

Handlers **load** loan / agent-session / compliance aggregates from their streams, **assert** invariants (state machine, model-version locking, compliance dependencies before binding approve), and **append** one batch. Correlation and causation metadata propagate for audit and replay narratives.

**Tradeoffs:** Domain rules live in application code (testable, versionable) rather than in-database triggers—operational risk is duplicated validation if a second writer path appears. The **loan-only append** for **`CreditAnalysisCompleted`** left the **agent** aggregate blind to that event when replaying **`agent-*`** alone; **`AgentContextLoaded`** was extended to **track `application_id`** so orchestration could still prove the session “saw” the application (see *What we would do differently*).

### 4. MCP, upcasting, and integrity surface

The MCP layer exposes **command tools** with **structured errors** (**`error_type`**, **`suggested_action`**, **`context`**). **Resources** resolve only through **`ProjectionQueryPort`**—`resource_*` methods do not call **`EventStore.load_stream` / `load_all`** directly. **Read paths** apply a registry-based **upcast** so persisted bytes stay immutable. **Integrity** verifies rolling hashes over **materialized** events against **`AuditIntegrityCheckRun`** checkpoints; regulatory packaging reuses the same verification and surfaces **`tamper_detected`**.

**Tradeoffs:** Upcast-on-read complicates “byte-identical row hashing” versus “logical fact hashing”; the integrity chain targets **logical** materialization for regulation-grade replay. **`InMemoryProjectionQueries`** replays inside the port for tests—fast, but it blurs the ideal boundary “projections only touch SQL read models.”

### 5. Quantitative reliability (error rates, lag SLOs, retry budgets)

| Mechanism | Concrete parameters | Residual / interpretation |
|-----------|---------------------|---------------------------|
| **PostgreSQL transient retry** | **`db_retry_max_attempts = 3`**, **`db_retry_base_delay = 0.05` s**, **`db_retry_max_delay = 2.0` s** (`EventStore` defaults) | If transient failure probability per attempt is \(p\), all attempts fail with probability \(p^{3}\). E.g. \(p=0.1\) → **~0.1%** tail failure; \(p=0.2\) → **~0.8%**. **`OptimisticConcurrencyError`** is **not** retried (`_run_with_db_retry` excludes it). |
| **Projection daemon handler retries** | **`max_retries = 3`** (constructor default) | Bounded re-apply; then checkpoint skips event (poison pill path). |
| **MCP optimistic concurrency** | **`max_occ_retries = 2`** (`LedgerMCPService` default → up to **3** attempts per tool body) | Contention-heavy tools return structured **`OptimisticConcurrencyError`** after exhaustion instead of blocking. |
| **Projection lag SLO** | **`LEDGER_PROJECTION_MAX_LAG_EVENTS`** default **100000**; override **`slo_max_lag_events=`** on **`get_health()`** | **`slo_projection_catchup_ok`** is false when **`max_projection_lag > ceiling`**; MCP **`resource_health`** mirrors **`slo_projection_catchup_ok`** and reports **`max_projection_lag`**. |
| **Integrity tool pacing** | **60 s** minimum between **`run_integrity_check`** calls per **`entity_type:entity_id`** | Caps load and audit chatter; second call returns **`RateLimited`**. |

**Tradeoffs:** More retries reduce user-visible failure under flaky networks but add **load amplification** during incidents. A **large lag ceiling** (100k events) avoids false positives during bulk replays but **delays** alerting if a projector is genuinely stuck.

### 6. Schema evolution and documented gaps

**`src/migrations/001_event_store.up.sql`** is aligned with **`src/schema.sql`**; **`001_event_store.down.sql`** orders drops to respect FKs. Column and index intent is spelled out in the **Schema Column Justification** section of this file; **Missing Elements Worth Adding** lists FK hardening, CHECK constraints, and idempotency keys—not implemented yet by design.

**Tradeoffs:** Migrations are the **contract** with production DDL; feature work that skips migrations is unsafe for regulated environments. Blue-green compliance rebuilds require an **explicit** daemon pause flag (**`LEDGER_COMPLIANCE_REBUILD_DAEMON_PAUSED=1`**)—correctness over convenience.

### What we would do differently

The implementation **got wrong** the **clear causal boundary between loan and agent streams**: credit completion is persisted **only** on **`loan-{id}`**, yet **`generate_decision`**’s Gas Town checks replay **`agent-{agent}-{session}`** and expected **`CreditAnalysisCompleted`** (or equivalent) to have updated the session aggregate. Shipping **`AgentContextLoaded`** with **`application_id`** and tracking it on the session fixed eligibility in practice, but it papers over an **asymmetric write pattern** that future contributors will find unintuitive. A **cleaner** approach would be a **small follow-on event on the agent stream** (or a **single transactional outbox-driven side effect**) emitted **after** a successful loan append, so each aggregate’s stream is **self-contained** under replay.

We would also make **`initiate_compliance_check.rules_to_evaluate`** **require an explicit policy stance** in production (even if “empty” is allowed with a named reason code), instead of defaulting to an empty list—empty rules simplify tests and human-binding overrides but **erase** audit clarity about which obligations were in scope.

---

## Rubric alignment

| Criterion | How the schema satisfies it |
|-----------|----------------------------|
| **Table completeness** | All four tables: `events`, `event_streams`, `projection_checkpoints`, `outbox`. |
| **Structural correctness** | `global_position` is `GENERATED ALWAYS AS IDENTITY` for total ordering; `uq_stream_position` enforces one row per `(stream_id, stream_position)`; `outbox.event_id` references `events(event_id)` for guaranteed linkage to persisted facts. |
| **Indexing strategy** | `events` indexes map to: stream replay `(stream_id, stream_position)`, global catch-up `(global_position)`, type-filtered reads `(event_type)`, time-window ops `(recorded_at)`, plus `BRIN(recorded_at)` for large append-only time scans; `event_streams` has `(aggregate_type, archived_at)` for active-stream listing. |
| **Lifecycle support** | `event_streams.archived_at` freezes streams; `outbox.published_at` and `outbox.attempts` support delivery tracking and retries. |

## Outbox reliability design

- `outbox.id` is independent from `event_id`, allowing fanout (`kafka:*`, `redis:*`, `webhook:*`) from one source event.
- `append()` writes `events` and `outbox` in one DB transaction; either both commit or both roll back.
- `idx_outbox_unpublished_created` (`WHERE published_at IS NULL`) supports efficient publisher polling.
- Publisher semantics are at-least-once: `claim_outbox_batch()` increments `attempts`, and success marks `published_at`.
- Destination idempotency is required because crashes can redeliver before `published_at` is persisted.

## Snapshot design

- `snapshots` stores aggregate state checkpoints by `stream_position`.
- `snapshot_version` is explicitly tracked and checked on load.
- If `snapshot_version != CURRENT_AGGREGATE_SNAPSHOT_VERSION`, snapshot is discarded and full replay is used.
- Latest snapshot lookup uses `idx_snapshots_stream_latest (stream_id, stream_position DESC)`.
- **Projection invalidation:** `ApplicationSummaryProjection._reset_state` deletes `snapshots` rows with `aggregate_type = 'loan'` before truncating `application_summary`, so stale JSON snapshots are not left pointing at discarded read-model rows. Periodic snapshots inside the projection handler remain best-effort diagnostics only.

## Projection daemon & CQRS (`src/projections/` — `daemon.py`, `application_summary.py`, `agent_performance.py`, `compliance_audit.py`, …)

| Area | Implementation |
|------|----------------|
| **Three projections** | `ApplicationSummaryProjection` (`application_summary`), `AgentPerformanceLedgerProjection` (`agent_performance_ledger`), `ComplianceAuditViewProjection` (`compliance_audit_view`). |
| **Lag** | Each `Projection.get_lag()` = `MAX(global_position)` − checkpoint; `ProjectionDaemon.get_lag()` returns all names; **`get_health()`** adds `max_projection_lag`, `event_store_head_global_position`, `slo_projection_catchup_ok` vs `LEDGER_PROJECTION_MAX_LAG_EVENTS` (override via `slo_max_lag_events=`). MCP **`SqlProjectionQueries.get_projection_lags`** exposes the same head−checkpoint map for ops dashboards. |
| **Fault tolerance** | Handler failures: bounded retries (`max_retries`), then **checkpoint advanced** to skip poison-pill events (at-least-once; handlers should be idempotent). Daemon loop is long-running + `stop()` for graceful shutdown. |
| **SLO tests** | `tests/phase3/test_projection_daemon.py` asserts `get_health` when lag exceeds the configured ceiling (`slo_projection_catchup_ok is False`). |
| **Rebuild compliance without stopping the log** | Blue-green tables `compliance_audit_current_bg`, `compliance_audit_snapshots_bg` (see `schema.sql` / `001_event_store.up.sql`). **`rebuild_compliance_audit_blue_green`**: full replay into `*_bg`, optional tail chase while writers append to `events`, **`ALTER TABLE … RENAME` swap** (sub-second metadata locks), then align `compliance_audit_view` checkpoint and truncate `*_bg`. **Requirement:** pause the daemon’s compliance leg (or whole daemon) during the rebuild so it does not mutate primary `compliance_audit_current` concurrently; set **`LEDGER_COMPLIANCE_REBUILD_DAEMON_PAUSED=1`**. The **event store stays up**; only the projector side pauses briefly. |
| **Temporal ComplianceAuditView** | **`ComplianceAuditViewProjection.get_compliance_at(application_id, timestamp)`**: nearest `compliance_audit_snapshots` row with `snapshot_at <= timestamp`, then replays `events` on `compliance-{application_id}` with `recorded_at <= timestamp` and `global_position` after the snapshot cursor. **`SqlProjectionQueries.get_compliance_as_of`** delegates to this method (replacing snapshot-only queries) so API/MCP match projector semantics. Postgres: `tests/phase3/test_projection_daemon.py::test_sql_compliance_as_of_matches_projection_temporal` (`pytest -m postgres_integration`). |

### Distributed / multi-instance daemon (analysis)

- **Checkpoint row** `projection_checkpoints` is a single logical cursor per `projection_name`. Safe scale-out requires **exactly one active consumer** per `(projection_name)` writer, or **idempotent** handlers with **partitioned** streams (not implemented here). Running two daemons with the same names will **double-apply** events unless sharded by `global_position` range.
- **Recommended patterns:** (1) single Kubernetes Deployment replica for `ProjectionDaemon`, (2) leader election (e.g. Postgres advisory lock) before `_process_batch`, (3) split projections across processes with distinct `projection_name` + disjoint event-type subscriptions if the model allows.
- **Catch-up:** Daemon uses **min** checkpoint across registered projections when reading `load_all`, so a **slow** projection back-pressures the shared batch cursor; lag per name in `get_health()` makes stragglers visible.

## EventStore implementation (`src/event_store.py`)

| Rubric | Implementation |
|--------|----------------|
| **Transactional append** | Single `asyncpg` transaction: CAS `UPDATE event_streams` → `INSERT events` → `INSERT outbox` per event. |
| **Optimistic concurrency** | `UPDATE … WHERE current_version = $expected`; `OptimisticConcurrencyError` if no row updated (never retried). |
| **`load_all` batching** | Long-lived connection; pages with `LIMIT batch_size`; cursor via `global_position`; `batch_size >= 1` enforced. |
| **Transient DB retry** | `_run_with_db_retry`: exponential backoff (`db_retry_base_delay * 2**attempt`, cap `db_retry_max_delay`); `is_transient_db_error` excludes OCC. See **Transient retry — success probability** below. |
| **Projection checkpoints** | `save_checkpoint` / `load_checkpoint` upsert `projection_checkpoints` (`ON CONFLICT`); default `0` when absent. |
| **Store contract** | `EventStoreLike` (`src/event_store.py`): methods shared by `EventStore` and `InMemoryEventStore` (excludes `connect`/`close`). |
| **Tests** | No DB: `tests/phase1/test_event_store_retry.py`, `tests/phase1/test_phase1_event_store.py`, `tests/phase1/test_concurrency.py`. PostgreSQL: `tests/phase1/test_event_store.py` (`pytest -m postgres_integration`); URL helpers in `tests/support/postgres_support.py` (`APEX_LEDGER_TEST_DB_URL` / `DATABASE_URL`). |

### Transient retry — success probability

Let each attempt succeed with probability \(q = 1 - p\) ( \(p\) = per-attempt failure rate for *transient* errors only). With `db_retry_max_attempts = n` and independent attempts, an operation eventually succeeds with probability \(1 - p^n\) (geometric cap). Example: \(p = 0.1\), \(n = 3\) → failure after all retries \(\approx 0.1\% \); \(p = 0.2\), \(n = 5\) → \(\approx 0.03\% \). Non-transient errors (including `OptimisticConcurrencyError`) are not retried: one attempt, no backoff.

## Command handler pattern (`src/commands/`)

| Rubric | Implementation |
|--------|------------------|
| **Orchestration** | Each `handle_*`: `load` loan and/or agent/compliance streams → guard methods → `store_dict_*` factories → single `append`. |
| **OCC + trace** | `expected_version` = replayed aggregate `version`; `correlation_id` / `causation_id` passed through to `append`. |
| **Observability** | `run_with_command_observability` (`observability.py`): `handler_start` / `handler_ok` (`duration_ms`) / `handler_failed`. Logger: **`ledger.commands`**. |
| **Tests** | `tests/phase2/test_command_handlers.py` (`pytest -m command_handler`). Details: `src/commands/README.md`. |

## Migrations and rollback

- **Forward:** `src/migrations/001_event_store.up.sql` — must match `src/schema.sql` DDL (contract for code and reviewers).
- **Rollback:** `src/migrations/001_event_store.down.sql` — drops `outbox` before `events` so the FK is respected; drops event indexes before tables.
- **Intent comments** for columns, constraints, and indexes live in `src/schema.sql` for maintainers; `DESIGN.md` summarizes justification.
- **CI:** `tests/phase1/test_schema_migrations.py` checks that migration files contain the required objects and safe drop order.

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
- **`idx_events_recorded_brin`:** read pattern — low-overhead coarse pruning for very large append-only time windows.

### `event_streams`
- `stream_id`: stream identity and lookup key.
- `aggregate_type`: partitioning/ops visibility by aggregate family.
- `current_version`: CAS target for optimistic concurrency checks.
- `created_at`: stream lifecycle origin.
- `archived_at`: lifecycle freeze marker; prevents further writes when archived.
- `metadata`: stream-level labels/ownership/context.

Indexes:
- **`idx_streams_aggregate_archived`:** read pattern — list active streams by type with `archived_at IS NULL`.

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

Indexes:
- **`idx_outbox_unpublished_created`:** polling unpublished rows in FIFO order (`published_at IS NULL`).

### `snapshots`
- `snapshot_id`: primary key for optional snapshot row (audit / delete-by-id if ever needed).
- `stream_id`: ties snapshot to aggregate stream; FK to `event_streams` so only registered streams hold snapshots.
- `stream_position`: last event position included in `state` (replay resumes after this).
- `aggregate_type`: mirrors stream family for ops filtering without parsing `stream_id`.
- `snapshot_version`: schema/serialization version of `state`; mismatch triggers full replay (see **Snapshot design** above).
- `state`: aggregate JSON blob at `stream_position`.
- `created_at`: audit and “staleness vs events” comparisons.

Indexes:
- **`idx_snapshots_stream_latest`:** `ORDER BY stream_position DESC` — O(1) pick of newest snapshot per stream.

### Read models and compliance tables (`application_summary` and related)

These columns exist for **CQRS-style projections** built from the event log (Phase 3). Each is justified as a denormalized query field; source of truth remains `events`.

**`application_summary`:** `application_id` (PK); `state` (current_fsm label); `applicant_id`; `requested_amount_usd` / `approved_amount_usd` (money); `risk_tier`; `fraud_score`; `compliance_status`; `decision`; `agent_sessions_completed` (JSON list); `last_event_type` / `last_event_at` (freshness); `human_reviewer_id`; `final_decision_at`. Index: `last_event_at` for time-sorted dashboards.

**`agent_performance_ledger`:** composite PK `(agent_id, model_version)`; counters `analyses_completed`, `decisions_generated`; rolling `avg_confidence_score`, `avg_duration_ms`; rate fields `approve_rate`, `decline_rate`, `refer_rate`, `human_override_rate`; `first_seen_at` / `last_seen_at` for lifecycle.

**`agent_session_index`:** `session_id` (PK); `application_id`, `agent_id`, `model_version` for reverse lookups from session → application.

**`application_decision_attribution`:** `application_id` (PK); `model_version` recording which model version produced the bound decision.

**`compliance_audit_current`:** `application_id` (PK); `regulation_set_version`; `checks` (JSON array); `verdict`; `latest_event_at`; `last_event_type`; `event_count` (replay depth hint).

**`compliance_audit_snapshots`:** `application_id`, `up_to_global_position` (PK pair); `snapshot_at`; `snapshot_payload` (point-in-time compliance JSON). Index: `application_id, snapshot_at DESC` for “latest snapshot before T”.

## Missing Elements Worth Adding (Future Hardening)

- `NOT NULL` and FK on `events.stream_id -> event_streams.stream_id` (requires migration strategy for bootstrap loads).
- `CHECK (stream_position > 0)` and `CHECK (current_version >= 0)` to lock in 1-based stream positions.
- `outbox` indexes: `(published_at, created_at)` and `(event_id, destination)` for publisher efficiency and idempotency.
- Trigger for `projection_checkpoints.updated_at` on update to avoid stale timestamps.
- Optional `idempotency_key` in `events.metadata` (or dedicated column) for exactly-once command dedupe.
- Optional archival policy metadata (`archived_reason`, `archived_by`) for regulated audits.

## Related

- **Aggregate design & state reconstruction:** `src/aggregates/*.py` (invariants in code); guard tests under `tests/phase2/test_*_aggregate.py`.
- **Command handlers & observability:** **Command handler pattern** table above; `src/commands/README.md`; `pytest -m command_handler`.
- **Postgres EventStore integration tests:** `tests/support/postgres_support.py` and `pytest -m postgres_integration` (tests skip when the DB URL is unreachable).
- **Domain events & factories:** `src/models/events.py`, `src/models/event_factories.py`; `tests/phase2/test_domain_event_models.py`.
- **EventStore code & tests:** `src/event_store.py` and the **EventStore implementation** table above.
