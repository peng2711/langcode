"""DAGScheduler integration tests against a real PostgreSQL.

Set TEST_POSTGRES_URI to run them, e.g.
    TEST_POSTGRES_URI=postgresql://postgres:postgres@127.0.0.1:5432/langcode_test
"""

from __future__ import annotations

import asyncio
import os
import uuid

import pytest
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from lib.dag_scheduler import DAGScheduler

TEST_POSTGRES_URI = os.getenv("TEST_POSTGRES_URI")

pytestmark = pytest.mark.skipif(
    not TEST_POSTGRES_URI, reason="TEST_POSTGRES_URI is not set"
)


def _run(scenario, max_attempts: int = 3, pool_size: int = 4):
    async def main():
        pool = AsyncConnectionPool(
            TEST_POSTGRES_URI,
            min_size=1,
            max_size=pool_size,
            kwargs={"autocommit": True, "row_factory": dict_row},
            open=False,
        )
        await pool.open()
        try:
            scheduler = DAGScheduler(pool, max_attempts=max_attempts)
            await scheduler.setup()
            await scenario(scheduler, pool, f"test-{uuid.uuid4().hex[:12]}")
        finally:
            await pool.close()

    asyncio.run(main())


async def _insert(scheduler: DAGScheduler, thread_id: str, tasks: list[dict]) -> None:
    for task in tasks:
        task.setdefault("subject", task["id"])
        task.setdefault("description", task["id"])
        task.setdefault("blockedBy", [])
    await scheduler.insert_dag_to_db({"tasks": tasks}, thread_id)


async def _expire_lease(pool: AsyncConnectionPool, task_id: str) -> None:
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE tasks SET lease_expires_at = NOW() - INTERVAL '1 second' WHERE id = %s",
            [task_id],
        )


async def _task(pool: AsyncConnectionPool, task_id: str) -> dict:
    async with pool.connection() as conn:
        cursor = await conn.execute("SELECT * FROM tasks WHERE id = %s", [task_id])
        return await cursor.fetchone()


def test_stale_owner_cannot_complete_after_lease_reclaimed():
    async def scenario(scheduler, pool, thread):
        root, child = f"{thread}-root", f"{thread}-child"
        await _insert(scheduler, thread, [{"id": root}, {"id": child, "blockedBy": [root]}])

        a = await scheduler.claim_next_available_task(thread, "agent-A")
        await _expire_lease(pool, root)
        assert await scheduler.reclaim_leased_tasks(thread) == 1
        b = await scheduler.claim_next_available_task(thread, "agent-B")
        assert (a["attempt"], b["attempt"]) == (1, 2)

        assert not await scheduler.renew_lease(root, "agent-A", a["attempt"])
        assert not await scheduler.complete_task(root, "agent-A", a["attempt"], "stale")
        assert await scheduler.fail_task(root, "agent-A", a["attempt"], "stale") is None

        assert await scheduler.complete_task(root, "agent-B", b["attempt"], "fresh")
        row = await _task(pool, root)
        assert row["status"] == "completed"
        assert row["metadata"]["summary"] == "fresh"
        assert (await _task(pool, child))["blocked_by_count"] == 0

    _run(scenario)


def test_same_owner_old_attempt_is_rejected():
    """owner alone is not a fence: the same agent may re-claim its own expired task."""

    async def scenario(scheduler, pool, thread):
        task_id = f"{thread}-t"
        await _insert(scheduler, thread, [{"id": task_id}])

        first = await scheduler.claim_next_available_task(thread, "agent-A")
        await _expire_lease(pool, task_id)
        await scheduler.reclaim_leased_tasks(thread)
        second = await scheduler.claim_next_available_task(thread, "agent-A")

        assert not await scheduler.complete_task(task_id, "agent-A", first["attempt"], "stale")
        assert await scheduler.complete_task(task_id, "agent-A", second["attempt"], "fresh")

    _run(scenario)


def test_fail_task_retries_until_max_attempts():
    async def scenario(scheduler, pool, thread):
        task_id = f"{thread}-t"
        await _insert(scheduler, thread, [{"id": task_id}])

        statuses = []
        while (claimed := await scheduler.claim_next_available_task(thread, "agent-A")):
            statuses.append(
                await scheduler.fail_task(task_id, "agent-A", claimed["attempt"], "boom")
            )

        assert statuses == ["pending", "pending", "failed"]
        row = await _task(pool, task_id)
        assert (row["status"], row["attempt"], row["owner"]) == ("failed", 3, None)
        assert row["metadata"]["last_error"] == "boom"

    _run(scenario)


def test_reclaim_fails_task_after_max_attempts_instead_of_leaving_it_stuck():
    async def scenario(scheduler, pool, thread):
        task_id = f"{thread}-t"
        await _insert(scheduler, thread, [{"id": task_id}])

        for _ in range(2):
            await scheduler.claim_next_available_task(thread, "agent-A")
            await _expire_lease(pool, task_id)
            assert await scheduler.reclaim_leased_tasks(thread) == 1

        row = await _task(pool, task_id)
        assert (row["status"], row["attempt"]) == ("failed", 2)
        assert await scheduler.claim_next_available_task(thread, "agent-A") is None

    _run(scenario, max_attempts=2)


def test_concurrent_claims_never_hand_out_a_task_twice():
    workers, tasks = 20, 200

    async def scenario(scheduler, pool, thread):
        await _insert(scheduler, thread, [{"id": f"{thread}-{i}"} for i in range(tasks)])
        claimed: list[str] = []

        async def worker(name: str):
            while (task := await scheduler.claim_next_available_task(thread, name)):
                claimed.append(task["id"])
                assert await scheduler.complete_task(task["id"], name, task["attempt"], "ok")

        await asyncio.gather(*(worker(f"agent-{i}") for i in range(workers)))

        assert len(claimed) == tasks
        assert len(set(claimed)) == tasks
        async with pool.connection() as conn:
            cursor = await conn.execute(
                "SELECT count(*) AS n FROM tasks WHERE thread_id = %s AND status = 'completed'",
                [thread],
            )
            assert (await cursor.fetchone())["n"] == tasks

    _run(scenario, pool_size=workers)
