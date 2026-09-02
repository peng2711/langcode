from __future__ import annotations

import asyncio
import json
import os
from uuid import uuid4

import pytest
from psycopg import errors
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from lib.dag_scheduler import DAGScheduler
from lib.db import get_postgres_uri


pytestmark = pytest.mark.skipif(
    os.getenv("LANGCODE_RUN_DB_TESTS") != "1",
    reason="set LANGCODE_RUN_DB_TESTS=1 with POSTGRES_* to run lifecycle integration tests",
)


async def _open_scheduler() -> tuple[AsyncConnectionPool, DAGScheduler]:
    pool = AsyncConnectionPool(
        get_postgres_uri(),
        min_size=1,
        max_size=4,
        kwargs={"autocommit": True, "row_factory": dict_row},
        open=False,
    )
    await pool.open()
    scheduler = DAGScheduler(pool, work_shard_count=4, lease_duration=30, max_attempts=3)
    await scheduler.setup()
    return pool, scheduler


async def _insert_card(
    pool: AsyncConnectionPool,
    *,
    card_id: str,
    task_type: str,
) -> None:
    async with pool.connection() as conn:
        await conn.execute(
            """
            INSERT INTO agent_cards (
                agent_card_id, version, status, task_types, system_prompt,
                tool_allowlist, skill_allowlist, runtime_config, bundle_ref,
                bundle_digest, config_hash
            )
            VALUES (
                %s, '1', 'active', %s::jsonb, 'integration test card',
                '[]'::jsonb, '[]'::jsonb, '{}'::jsonb, 'builtin:test',
                'digest-test', 'config-test'
            )
            """,
            [card_id, json.dumps([task_type])],
        )


async def _cleanup(
    pool: AsyncConnectionPool,
    *,
    task_id: str,
    card_id: str,
    runtime_ids: list[str],
) -> None:
    async with pool.connection() as conn:
        await conn.execute(
            """
            UPDATE tasks
            SET current_execution_id = NULL, current_attempt = NULL
            WHERE id = %s
            """,
            [task_id],
        )
        await conn.execute(
            "DELETE FROM task_execution_events WHERE task_id = %s", [task_id]
        )
        await conn.execute(
            "DELETE FROM message_outbox WHERE envelope->>'task_id' = %s", [task_id]
        )
        await conn.execute("DELETE FROM task_dependencies WHERE task_id = %s", [task_id])
        await conn.execute("DELETE FROM task_executions WHERE task_id = %s", [task_id])
        await conn.execute("DELETE FROM tasks WHERE id = %s", [task_id])
        await conn.execute(
            "DELETE FROM agent_runtimes WHERE runtime_id = ANY(%s)", [runtime_ids]
        )
        await conn.execute(
            "DELETE FROM agent_cards WHERE agent_card_id = %s", [card_id]
        )


def test_execution_lifecycle_has_deterministic_event_order() -> None:
    async def scenario() -> None:
        pool, scheduler = await _open_scheduler()
        suffix = uuid4().hex
        task_id = f"lifecycle-{suffix}"
        card_id = f"card-{suffix}"
        runtime_id = f"runtime-{suffix}"
        try:
            await _insert_card(pool, card_id=card_id, task_type="integration.lifecycle")
            await scheduler.register_runtime(runtime_id)
            await scheduler.insert_dag_to_db(
                {
                    "tasks": [
                        {
                            "id": task_id,
                            "subject": "verify lifecycle",
                            "task_type": "integration.lifecycle",
                            "card_selector": {
                                "agent_card_id": card_id,
                                "version": "1",
                            },
                        }
                    ]
                },
                thread_id=f"thread-{suffix}",
            )

            claim = await scheduler.claim_next_available_task(
                thread_id=f"thread-{suffix}",
                runtime_id=runtime_id,
                task_type="integration.lifecycle",
                work_shard=scheduler.work_shard_for(
                    task_id, "integration.lifecycle"
                ),
            )
            assert claim is not None
            assert await scheduler.mark_card_load_started(
                task_id=claim.task_id,
                execution_id=claim.execution_id,
                attempt=claim.attempt,
                runtime_id=runtime_id,
            )
            assert await scheduler.mark_execution_started(
                task_id=claim.task_id,
                execution_id=claim.execution_id,
                attempt=claim.attempt,
                runtime_id=runtime_id,
            )
            assert await scheduler.renew_lease(
                task_id=claim.task_id,
                execution_id=claim.execution_id,
                attempt=claim.attempt,
                runtime_id=runtime_id,
            )
            assert await scheduler.complete_execution(
                task_id=claim.task_id,
                execution_id=claim.execution_id,
                attempt=claim.attempt,
                runtime_id=runtime_id,
                summary="done",
            )
            assert await scheduler.finish_card_unload(
                task_id=claim.task_id,
                execution_id=claim.execution_id,
                attempt=claim.attempt,
                runtime_id=runtime_id,
            )
            assert not await scheduler.finish_card_unload(
                task_id=claim.task_id,
                execution_id=claim.execution_id,
                attempt=claim.attempt,
                runtime_id=runtime_id,
            )

            events = await scheduler.get_execution_events(
                claim.execution_id, claim.attempt
            )
            assert [event["event_type"] for event in events] == [
                "execution.claimed",
                "card.resolved",
                "card.load_started",
                "card.loaded",
                "execution.started",
                "execution.heartbeat",
                "execution.completed",
                "card.unload_started",
                "card.unloaded",
            ]
            assert [event["sequence_no"] for event in events] == sorted(
                event["sequence_no"] for event in events
            )
        finally:
            await _cleanup(
                pool,
                task_id=task_id,
                card_id=card_id,
                runtime_ids=[runtime_id],
            )
            await pool.close()

    asyncio.run(scenario())


def test_retry_reuses_execution_and_late_completion_is_rejected() -> None:
    async def scenario() -> None:
        pool, scheduler = await _open_scheduler()
        suffix = uuid4().hex
        task_id = f"retry-{suffix}"
        card_id = f"card-{suffix}"
        first_runtime = f"runtime-first-{suffix}"
        second_runtime = f"runtime-second-{suffix}"
        try:
            await _insert_card(pool, card_id=card_id, task_type="integration.retry")
            await scheduler.register_runtime(first_runtime)
            await scheduler.register_runtime(second_runtime)
            await scheduler.insert_dag_to_db(
                {
                    "tasks": [
                        {
                            "id": task_id,
                            "subject": "verify retry",
                            "task_type": "integration.retry",
                            "card_selector": {
                                "agent_card_id": card_id,
                                "version": "1",
                            },
                        }
                    ]
                },
                thread_id=f"thread-{suffix}",
            )
            shard = scheduler.work_shard_for(task_id, "integration.retry")
            first_claim = await scheduler.claim_next_available_task(
                thread_id=f"thread-{suffix}",
                runtime_id=first_runtime,
                task_type="integration.retry",
                work_shard=shard,
            )
            assert first_claim is not None
            assert await scheduler.mark_execution_started(
                task_id=task_id,
                execution_id=first_claim.execution_id,
                attempt=first_claim.attempt,
                runtime_id=first_runtime,
            )
            decision = await scheduler.fail_execution(
                task_id=task_id,
                execution_id=first_claim.execution_id,
                attempt=first_claim.attempt,
                runtime_id=first_runtime,
                error="temporary failure",
            )
            assert decision.retry_scheduled
            assert await scheduler.finish_card_unload(
                task_id=task_id,
                execution_id=first_claim.execution_id,
                attempt=first_claim.attempt,
                runtime_id=first_runtime,
            )

            second_claim = await scheduler.claim_next_available_task(
                thread_id=f"thread-{suffix}",
                runtime_id=second_runtime,
                task_type="integration.retry",
                work_shard=shard,
            )
            assert second_claim is not None
            assert second_claim.execution_id == first_claim.execution_id
            assert second_claim.attempt == first_claim.attempt + 1
            assert await scheduler.mark_execution_started(
                task_id=task_id,
                execution_id=second_claim.execution_id,
                attempt=second_claim.attempt,
                runtime_id=second_runtime,
            )
            assert not await scheduler.complete_execution(
                task_id=task_id,
                execution_id=first_claim.execution_id,
                attempt=first_claim.attempt,
                runtime_id=first_runtime,
                summary="late result",
            )
            assert await scheduler.cancel_execution(
                task_id=task_id,
                execution_id=second_claim.execution_id,
                attempt=second_claim.attempt,
                runtime_id=second_runtime,
                reason="integration cancellation",
            )
            assert await scheduler.finish_card_unload(
                task_id=task_id,
                execution_id=second_claim.execution_id,
                attempt=second_claim.attempt,
                runtime_id=second_runtime,
            )

            history = await scheduler.get_task_execution_history(task_id)
            assert [
                (str(record["execution_id"]), record["attempt"], record["status"])
                for record in history
            ] == [
                (first_claim.execution_id, 1, "failed"),
                (first_claim.execution_id, 2, "cancelled"),
            ]
            first_events = await scheduler.get_execution_events(
                first_claim.execution_id, first_claim.attempt
            )
            assert "execution.stale_update_rejected" in {
                event["event_type"] for event in first_events
            }
        finally:
            await _cleanup(
                pool,
                task_id=task_id,
                card_id=card_id,
                runtime_ids=[first_runtime, second_runtime],
            )
            await pool.close()

    asyncio.run(scenario())


def test_published_agent_card_content_is_immutable() -> None:
    async def scenario() -> None:
        pool, _ = await _open_scheduler()
        suffix = uuid4().hex
        card_id = f"card-{suffix}"
        try:
            await _insert_card(pool, card_id=card_id, task_type="integration.card")
            async with pool.connection() as conn:
                with pytest.raises(errors.RaiseException, match="immutable"):
                    await conn.execute(
                        """
                        UPDATE agent_cards
                        SET system_prompt = 'mutated'
                        WHERE agent_card_id = %s AND version = '1'
                        """,
                        [card_id],
                    )
                await conn.execute(
                    """
                    UPDATE agent_cards
                    SET status = 'deprecated'
                    WHERE agent_card_id = %s AND version = '1'
                    """,
                    [card_id],
                )
        finally:
            await _cleanup(
                pool,
                task_id=f"unused-{suffix}",
                card_id=card_id,
                runtime_ids=[],
            )
            await pool.close()

    asyncio.run(scenario())
