# The Ledger — Event-Sourced Loan Decision Platform

This repository contains an event-sourced backbone for The Ledger challenge, including:
- typed domain events
- PostgreSQL and in-memory event stores
- replay-based aggregates and command handlers
- agent scaffolding for document-to-decision workflow

## Current implementation status

### Completed
- **Phase 1 (Event Store Core)**
  - `ledger/schema.sql` with required tables: `events`, `event_streams`, `projection_checkpoints`, `outbox`
  - `ledger/event_store.py` with async interface:
    - `append`
    - `load_stream`
    - `load_all`
    - `stream_version`
    - `archive_stream`
    - `get_stream_metadata`
  - Transactional append with outbox write and database-level optimistic concurrency
  - Transient DB retry/backoff (`_run_with_db_retry`, `tests/test_event_store_retry.py`)
  - In-memory + OCC tests: `tests/phase1/test_event_store.py`, `tests/test_concurrency.py`
  - Postgres integration (empty/`load_all` batching/concurrency): `tests/test_event_store.py`, `docs/local_postgres_testing.md`

- **Phase 2 (Domain Logic)**
  - Replay-based aggregates:
    - `ledger/domain/aggregates/loan_application.py`
    - `ledger/domain/aggregates/agent_session.py`
    - `ledger/domain/aggregates/compliance_record.py`
  - Command handlers in `ledger/domain/commands/handlers.py` (load → guards → `store_dict_*` → append + trace ids; `run_with_command_observability` / `ledger.commands`)
  - Domain rule enforcement includes:
    - valid state transitions
    - Gas Town context requirement
    - model-version locking with override path
    - confidence floor (`< 0.6` => `REFER`)
    - compliance dependency
    - causal chain checks for contributing sessions

- **Event and schema layer**
  - Canonical typed models in `ledger/schema/events.py`
  - Upcaster registry in `ledger/upcasters.py`

- **Document processing agent implementation**
  - Production `DocumentProcessingAgent` in `ledger/agents/document_processing_agent.py`
  - Backward-compatible export from `ledger/agents/base_agent.py`

### In progress / stubs
- `ledger/registry/client.py` (Applicant Registry query methods)
- Phase 3+ agents in `ledger/agents/stub_agents.py` (Fraud, Compliance, Orchestrator)
- Projections daemon and MCP server layers

## Quick start

```bash
# 1) Install dependencies
pip install -r requirements.txt

# 2) Start PostgreSQL (dev DB; use apex_ledger_test for pytest — see docs/local_postgres_testing.md)
docker run -d -e POSTGRES_PASSWORD=apex -e POSTGRES_DB=apex_ledger -p 5432:5432 postgres:16

# 3) Configure environment
cp .env.example .env

# 4) Generate data and seed events
python datagen/generate_all.py --db-url postgresql://postgres:apex@localhost/apex_ledger
```

## Useful test commands

```bash
# Schema and generator validation
pytest tests/test_schema_and_generator.py -v

# Phase 1 in-memory/event-store behavior + OCC patterns
pytest tests/phase1/test_event_store.py -v
pytest tests/test_concurrency.py -v

# Command handlers (guards, append contract, observability)
pytest tests/test_command_handlers.py -v
pytest -m command_handler -v

# EventStore retry / batch validation (no Postgres required)
pytest tests/test_event_store_retry.py -v

# PostgreSQL EventStore integration (auto-skips if DB unreachable)
# Optional: export APEX_LEDGER_TEST_DB_URL=postgresql://postgres:apex@127.0.0.1:5432/apex_ledger_test
pytest tests/test_event_store.py -v
pytest -m postgres_integration -v
```

## Project structure (key paths)

- `ledger/schema/events.py` — typed domain events and exceptions
- `ledger/schema.sql` — relational event store schema
- `ledger/event_store.py` — Postgres + in-memory store implementations
- `ledger/domain/aggregates/` — replay-based aggregate models
- `ledger/domain/commands/handlers.py` — command handling entrypoints
- `ledger/agents/` — base agent, document processing, and stubs
- `datagen/` — deterministic dataset and event generation utilities
