import asyncio

import asyncpg.exceptions as apg_exc
import pytest

from ledger.event_store import EventStore, is_transient_db_error
from ledger.schema.events import OptimisticConcurrencyError


def test_is_transient_deadlock_and_serialization():
    assert is_transient_db_error(apg_exc.DeadlockDetectedError("deadlock"))
    assert is_transient_db_error(apg_exc.SerializationError("ser"))


def test_is_transient_not_optimistic_concurrency():
    assert not is_transient_db_error(
        OptimisticConcurrencyError("loan-x", 1, 2),
    )


def test_is_transient_not_key_error():
    assert not is_transient_db_error(KeyError("x"))


@pytest.mark.asyncio
async def test_run_with_db_retry_exponential_backoff_succeeds():
    store = EventStore(
        "postgresql://unused",
        db_retry_max_attempts=4,
        db_retry_base_delay=0.01,
        db_retry_max_delay=0.5,
    )
    calls = {"n": 0}

    async def flaky() -> int:
        calls["n"] += 1
        if calls["n"] < 3:
            raise apg_exc.DeadlockDetectedError("retry me")
        return 7

    result = await store._run_with_db_retry(flaky)
    assert result == 7
    assert calls["n"] == 3


@pytest.mark.asyncio
async def test_run_with_db_retry_does_not_retry_occ():
    store = EventStore(
        "postgresql://unused",
        db_retry_max_attempts=5,
        db_retry_base_delay=0.01,
    )
    calls = {"n": 0}

    async def always_occ() -> int:
        calls["n"] += 1
        raise OptimisticConcurrencyError("s", 1, 2)

    with pytest.raises(OptimisticConcurrencyError):
        await store._run_with_db_retry(always_occ)
    assert calls["n"] == 1


@pytest.mark.asyncio
async def test_run_with_db_retry_exhausts_then_raises():
    store = EventStore(
        "postgresql://unused",
        db_retry_max_attempts=2,
        db_retry_base_delay=0.01,
    )

    async def always_deadlock() -> None:
        raise apg_exc.DeadlockDetectedError("x")

    with pytest.raises(apg_exc.DeadlockDetectedError):
        await store._run_with_db_retry(always_deadlock)


@pytest.mark.asyncio
async def test_run_with_db_retry_disabled_single_attempt():
    store = EventStore("postgresql://unused", db_retry_max_attempts=1)

    async def ok() -> str:
        return "ok"

    assert await store._run_with_db_retry(ok) == "ok"


@pytest.mark.asyncio
async def test_load_all_rejects_non_positive_batch_size_without_connecting():
    store = EventStore("postgresql://unused")
    with pytest.raises(ValueError, match="batch_size"):
        async for _ in store.load_all(batch_size=0):
            pass


def test_load_all_rejects_negative_batch_in_memory():
    from ledger.event_store import InMemoryEventStore

    async def drain() -> None:
        s = InMemoryEventStore()
        async for _ in s.load_all(batch_size=-1):
            pass

    with pytest.raises(ValueError, match="batch_size"):
        asyncio.run(drain())
