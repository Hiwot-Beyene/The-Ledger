# The Ledger — Event-Sourced Loan Decision Platform

## System overview

**Apex Ledger** is an event-sourced backbone for credit operations: every intake, document, and decision is recorded as an **immutable fact** on the ledger. A **Staff Portal** (React) and **HTTP API** orchestrate applications against PostgreSQL-backed **projections** (e.g. `application_summary`), with optional **MCP** tools for agentic workflows and audit-facing regulatory packages.

![Apex Ledger Staff Portal — command center for event-sourced credit](image.png)

*Staff dashboard: command center, pipeline, and navigator; projections served from Postgres.*

**Layout:** Application code lives under **`src/`** using **direct modules** (with `pythonpath=src`): for example `from event_store import EventStore`, `from models.events import StoredEvent`, `from aggregates.loan_application import LoanApplicationAggregate`, `from projections import ProjectionDaemon`, `from mcp.server import LedgerMCPService`. The Staff HTTP API is **`src/api/`**. Tests and `pytest` add `src` to `pythonpath` (see `pytest.ini`); CLIs prepend `src` on `sys.path` as well.

---

## Full setup

### Prerequisites

- **Python 3.11+**
- **PostgreSQL 16** (local or Docker)
- **Node.js 18+** (only if you run the `portal/` UI)

### 1. Install dependencies

```bash
pip install -r requirements.txt
# or: pip install -e .
```

### 2. Database provisioning

Create databases for the app and for integration tests (names can match your `.env`).

```bash
# Example: Docker Postgres on port 5432
docker run -d --name apex-pg \
  -e POSTGRES_PASSWORD=apex \
  -e POSTGRES_DB=apex_ledger \
  -p 5432:5432 \
  postgres:16

# Optional: separate test database
psql "postgresql://postgres:apex@127.0.0.1:5432/postgres" -c "CREATE DATABASE apex_ledger_test;"
```

`.env.example` uses port **5434** in one place; align `DATABASE_URL` with whatever port you map.

### 3. Configure environment

```bash
cp .env.example .env
# Edit .env — set at least:
#   DATABASE_URL=postgresql://postgres:apex@127.0.0.1:5432/apex_ledger
#   APPLICANT_REGISTRY_URL=(often same as DATABASE_URL for dev)
```

### 4. Run migrations

Apply the event store DDL (matches `src/schema.sql`).

```bash
export DATABASE_URL=postgresql://postgres:apex@127.0.0.1:5432/apex_ledger
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f src/migrations/001_event_store.up.sql
```

Rollback (dev only):

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f src/migrations/001_event_store.down.sql
```

See `src/migrations/README.md` for guarantees and ordering.

### 5. Seed data (optional)

```bash
python datagen/generate_all.py --db-url "$DATABASE_URL"
```

### 6. Run the test suite (all phases)

From the repo root:

```bash
# All automated tests except optional postgres_integration skips
pytest tests -q --ignore=tests/portal

# By phase
pytest tests/phase1/ -v   # event store, schema, migrations, concurrency, retries
pytest tests/phase2/ -v   # aggregates, command handlers, domain
pytest tests/phase3/ -v   # projection daemon, read models
pytest tests/phase4/ -v   # upcasting, integrity
pytest tests/phase5/ -v   # MCP service / tools
pytest tests/phase6/ -v   # what-if, regulatory package, time travel

# Markers
pytest -m command_handler -v
pytest -m postgres_integration -v   # requires reachable DB; see below
```

**PostgreSQL integration tests** need a URL. If `DATABASE_URL` points at your dev DB, create `apex_ledger_test` and either:

```bash
export APEX_LEDGER_TEST_DB_URL=postgresql://postgres:apex@127.0.0.1:5432/apex_ledger_test
pytest -m postgres_integration -v
```

Helpers: `tests/support/postgres_support.py` (schema ensure, truncate). Default fallback URL is in that module.

---

## Staff HTTP API and portal

```bash
export DATABASE_URL=postgresql://postgres:apex@127.0.0.1:5432/apex_ledger
uvicorn src.api.app:app --reload --host 0.0.0.0 --port 8080
```

- **CORS:** override with `STAFF_API_CORS_ORIGINS` if the portal runs on a different origin than `http://localhost:5174`.

**Portal (React):**

```bash
cd portal && npm install && npm run dev
# Point API: set VITE_API_URL=http://localhost:8080 in portal env (e.g. `.env.local`)
```

Without `VITE_API_URL`, the UI can use embedded `data/*.json` for offline demos.

---

## MCP server (stdio)

The MCP server uses **PostgreSQL** `EventStore` and **SQL projection queries** for resources.

```bash
export DATABASE_URL=postgresql://postgres:apex@127.0.0.1:5432/apex_ledger
python scripts/run_mcp_server.py
```

Wire this command into **Claude Desktop**, **Cursor**, or any MCP client that launches a **stdio** server. Tools are described in `src/mcp/tools.py`; **`build_fastmcp_server`** lives in `src/mcp/server.py` (resources in `src/mcp/resources.py`): submit workflow, credit/compliance/decision, integrity, what-if, regulatory package, projection-backed `ledger://` resources.

---

## Query examples

### HTTP API (`curl`)

Assumes API on `http://localhost:8080` and a known `APPLICATION_ID`.

```bash
# Applicant registry (CRM boundary)
curl -s "http://localhost:8080/api/registry/companies" | head -c 500

# Pipeline from application_summary projection (when populated)
curl -s "http://localhost:8080/api/applications/pipeline"

# Single application dossier
curl -s "http://localhost:8080/api/applications/APPLICATION_ID/summary"

# Submit new application (body must match API schema; applicant_id must exist in registry)
curl -s -X POST "http://localhost:8080/api/applications" \
  -H "Content-Type: application/json" \
  -d '{"application_id":"demo-1","applicant_id":"...","requested_amount_usd":"10000"}'

# Navigator-style natural language → decision history package
curl -s -X POST "http://localhost:8080/api/navigator/decision-history" \
  -H "Content-Type: application/json" \
  -d '{"query":"Regulatory package for loan-demo-1 complete history"}'

# Compliance as-of (temporal projection); use ISO-8601
curl -s "http://localhost:8080/api/applications/APPLICATION_ID/compliance?as_of=2026-03-01T12:00:00%2B00:00"

# What-if (seeded app with MEDIUM credit → HIGH counterfactual; requires events in DB)
curl -s "http://localhost:8080/api/applications/APEX-0020/ledger/what-if-medium-to-high/hint"
curl -s -X POST "http://localhost:8080/api/applications/APEX-0020/ledger/what-if-medium-to-high"

# Generic what-if (counterfactual_events must be non-empty — see tests/phase6/test_time_travel.py)
# curl -s -X POST "http://localhost:8080/api/ledger/what-if" -H "Content-Type: application/json" -d @what_if_body.json

# Rolling integrity check (compliance-style caller in production)
curl -s -X POST "http://localhost:8080/api/ledger/integrity-check" \
  -H "Content-Type: application/json" \
  -d '{"entity_type":"loan","entity_id":"demo-1"}'
```

More routes are documented in `src/api/app.py` (module docstring).

### SQL (read models and ops)

```sql
-- Projection row (replace APPLICATION_ID)
SELECT * FROM application_summary WHERE application_id = 'APPLICATION_ID';

SELECT projection_name, last_position FROM projection_checkpoints;

SELECT COALESCE(MAX(global_position), 0) AS head FROM events;
```

### MCP resources (URIs)

When using an MCP client, resources are addressed as:

- `ledger://applications/{application_id}`
- `ledger://applications/{application_id}/compliance` (optional `as_of` query string where the client supports it)
- `ledger://applications/{application_id}/audit-trail`
- `ledger://agents/{agent_id}/performance`
- `ledger://agents/{agent_id}/sessions/{session_id}`
- `ledger://ledger/health`

---

## Implementation map

| Area | Location |
|------|----------|
| Event store (Postgres + in-memory) | `src/event_store.py` |
| Domain events (Pydantic) | `src/models/events.py`, `src/models/event_factories.py` |
| Schema & migrations | `src/schema.sql`, `src/migrations/` |
| Aggregates | `src/aggregates/` |
| Command handlers | `src/commands/handlers.py` |
| Projections & daemon | `src/projections/` (`daemon.py`, `application_summary.py`, …) |
| Upcasting | `src/upcasting/registry.py`, `src/upcasting/upcasters.py` |
| Integrity & Gas Town | `src/integrity/audit_chain.py`, `src/integrity/gas_town.py` |
| What-if & regulatory | `src/what_if/projector.py`, `src/regulatory/package.py`, `src/what_if_credit_high.py` |
| MCP service | `src/mcp/server.py`, `src/mcp/resources.py`, `src/mcp/tools.py`, `scripts/run_mcp_server.py` |
| Staff REST API | `src/api/` |
| Architecture write-up | `DESIGN.md` |
</think>


<｜tool▁calls▁begin｜><｜tool▁call▁begin｜>
StrReplace