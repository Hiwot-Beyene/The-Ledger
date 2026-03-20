# Command handlers

Application services: load aggregates by replay → guards → pure event payloads → `append(..., expected_version=agg.version, correlation_id=..., causation_id=...)`.

## Observability

All handlers run inside `run_with_command_observability` (`observability.py`), logger **`ledger.commands`**:

- `handler_start` / `handler_ok` (includes `duration_ms`) / `handler_failed` (with traceback)

Configure in app wiring, e.g. `logging.getLogger("ledger.commands").setLevel(logging.INFO)`.

## Tests

`tests/test_command_handlers.py` — per-handler happy paths, guard failures (duplicate submit, credit state, model version, Gas Town, decision causal chain, compliance), load order, `correlation_id` / `causation_id`, observability (`caplog`).

```bash
pytest tests/test_command_handlers.py -v
pytest -m command_handler -v
```
