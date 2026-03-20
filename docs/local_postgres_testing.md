# PostgreSQL for EventStore integration tests

## Why tests skip

`tests/test_event_store.py` talks to a real database. If Postgres is not running, the URL is wrong, or the database does not exist, **asyncpg cannot connect**. The test fixture catches that and calls `pytest.skip(...)` so the run stays green instead of failing with connection errors.

That is different from “tests disabled”: with a working DB, the same file **runs** all integration cases (OCC concurrency, `load_all` batching, empty streams, etc.).

## Make Postgres available

### 1. Run PostgreSQL (Docker example)

```bash
docker run -d --name apex-pg \
  -e POSTGRES_PASSWORD=apex \
  -e POSTGRES_DB=apex_ledger_test \
  -p 5432:5432 \
  postgres:16
```

Use a **dedicated test database** (`apex_ledger_test`) so `TRUNCATE` in tests never wipes dev data.

### 2. Point tests at the URL

Priority order:

1. `APEX_LEDGER_TEST_DB_URL`
2. `TEST_DB_URL` (see `tests/conftest.py`)
3. `DATABASE_URL`
4. Default: `postgresql://postgres:apex@127.0.0.1:5432/apex_ledger_test`

Example:

```bash
export APEX_LEDGER_TEST_DB_URL='postgresql://postgres:apex@127.0.0.1:5432/apex_ledger_test'
pytest tests/test_event_store.py -v
```

### 3. Schema

On first successful connection, tests run `ledger/schema.sql` if the `events` table is missing (requires `CREATE EXTENSION` permission for `pgcrypto`).

You can also apply DDL yourself:

```bash
psql "$APEX_LEDGER_TEST_DB_URL" -f ledger/schema.sql
```

### 4. Local Postgres without Docker

Install PostgreSQL, create a role and database, set `APEX_LEDGER_TEST_DB_URL`, apply `ledger/schema.sql`, then run `pytest tests/test_event_store.py`.

## Marker

Tests are marked `postgres_integration` (see `pytest.ini`). To run only these:

```bash
pytest -m postgres_integration -v
```

## CI

In CI, start a Postgres service, set `APEX_LEDGER_TEST_DB_URL`, and run `pytest tests/test_event_store.py` (or `-m postgres_integration`) so integration tests execute instead of skipping.
