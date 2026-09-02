"""PostgreSQL source-of-truth scheduler for DAGs and execution attempts."""

from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
from uuid import uuid4

from psycopg_pool import AsyncConnectionPool

from lib.message_hub import (
    MessageEnvelope,
    build_envelope,
    enqueue_outbox_event,
)

logger = logging.getLogger(__name__)

DEFAULT_WORK_SHARDS = 64
DEFAULT_LEASE_SECONDS = 60


@dataclass(frozen=True)
class ClaimedExecution:
    """Task and frozen Agent Card snapshot returned after an atomic claim."""

    task_id: str
    subject: str
    description: str | None
    thread_id: str | None
    task_type: str
    work_shard: int
    metadata: dict[str, Any]
    execution_id: str
    attempt: int
    runtime_id: str
    agent_card_id: str
    agent_card_version: str
    agent_card_digest: str
    agent_card_config_hash: str
    card_system_prompt: str
    card_tool_allowlist: list[str]
    card_skill_allowlist: list[str]
    card_runtime_config: dict[str, Any]
    card_bundle_ref: str


@dataclass(frozen=True)
class RetryDecision:
    retry_scheduled: bool
    execution_id: str
    attempt: int


def _row_to_dict(cursor: Any, row: Any) -> dict[str, Any] | None:
    if row is None:
        return None
    if isinstance(row, dict):
        return dict(row)
    return dict(zip([desc.name for desc in cursor.description], row))


def _as_json_object(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, str):
        return json.loads(value)
    return dict(value)


def _as_json_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return list(json.loads(value))
    return list(value)


class DAGScheduler:
    """Atomic DAG publication, claims, execution lifecycle, and audit events."""

    def __init__(
        self,
        pool: AsyncConnectionPool,
        *,
        work_shard_count: int | None = None,
        lease_duration: int = DEFAULT_LEASE_SECONDS,
        max_attempts: int | None = None,
    ):
        self.pool = pool
        self.work_shard_count = work_shard_count or int(
            os.getenv("TASK_WORK_SHARD_COUNT", str(DEFAULT_WORK_SHARDS))
        )
        self.lease_duration = lease_duration
        self.max_attempts = max_attempts or int(
            os.getenv("TASK_MAX_ATTEMPTS", "2")
        )
        if self.work_shard_count < 1:
            raise ValueError("work_shard_count must be >= 1")

    async def setup(self) -> None:
        """Apply the schema shared by the scheduler, Outbox, and audit systems."""
        schema_path = Path(__file__).resolve().parents[1] / "schema.sql"
        async with self.pool.connection() as conn:
            await conn.execute(schema_path.read_text(encoding="utf-8"))

    def work_shard_for(self, task_id: str, task_type: str = "general") -> int:
        """Compute a stable, process-independent task shard."""
        digest = hashlib.sha256(
            f"{task_type}:{task_id}".encode("utf-8")
        ).digest()
        return int.from_bytes(digest[:8], "big") % self.work_shard_count

    async def register_runtime(
        self,
        runtime_id: str,
        *,
        runtime_version: str = "v1",
    ) -> None:
        async with self.pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO agent_runtimes
                    (runtime_id, status, runtime_version, last_heartbeat,
                     lease_expires_at, started_at)
                VALUES (%s, 'idle', %s, NOW(),
                        NOW() + (%s * INTERVAL '1 second'), NOW())
                ON CONFLICT (runtime_id) DO UPDATE
                SET status = 'idle',
                    runtime_version = EXCLUDED.runtime_version,
                    current_execution_id = NULL,
                    current_attempt = NULL,
                    last_heartbeat = NOW(),
                    lease_expires_at = EXCLUDED.lease_expires_at
                """,
                [runtime_id, runtime_version, self.lease_duration],
            )

    async def heartbeat_runtime(self, runtime_id: str) -> bool:
        async with self.pool.connection() as conn:
            cursor = await conn.execute(
                """
                UPDATE agent_runtimes
                SET last_heartbeat = NOW(),
                    lease_expires_at = NOW() + (%s * INTERVAL '1 second')
                WHERE runtime_id = %s AND status <> 'stopped'
                """,
                [self.lease_duration, runtime_id],
            )
            return cursor.rowcount > 0

    async def stop_runtime(self, runtime_id: str) -> None:
        async with self.pool.connection() as conn:
            await conn.execute(
                """
                UPDATE agent_runtimes
                SET status = 'stopped', lease_expires_at = NOW()
                WHERE runtime_id = %s
                """,
                [runtime_id],
            )

    async def get_runtime_execution(
        self,
        runtime_id: str,
    ) -> dict[str, Any] | None:
        """Return the Runtime's current execution context, if it has one."""
        async with self.pool.connection() as conn:
            cursor = await conn.execute(
                """
                SELECT r.runtime_id, r.status, e.task_id, e.execution_id, e.attempt
                FROM agent_runtimes r
                LEFT JOIN task_executions e
                  ON e.execution_id = r.current_execution_id
                 AND e.attempt = r.current_attempt
                WHERE r.runtime_id = %s
                """,
                [runtime_id],
            )
            return _row_to_dict(cursor, await cursor.fetchone())

    async def insert_dag_to_db(
        self,
        dag_data: dict[str, Any],
        thread_id: str,
        owner: Optional[str] = None,
    ) -> dict[str, Any]:
        """Publish DAG rows and their initial wakeup events in one transaction."""
        tasks = dag_data.get("tasks", [])
        if not tasks:
            return {"tasks_inserted": 0, "dependencies_inserted": 0}

        async with self.pool.connection() as conn:
            async with conn.transaction():
                tasks_inserted = 0
                dependencies_inserted = 0
                ready_tasks: list[dict[str, Any]] = []

                for task in tasks:
                    task_id = task["id"]
                    task_type = task.get("task_type") or task.get("taskType") or "general"
                    work_shard = int(
                        task.get(
                            "work_shard",
                            task.get(
                                "workShard",
                                self.work_shard_for(task_id, task_type),
                            ),
                        )
                    )
                    if work_shard < 0:
                        raise ValueError(f"task {task_id} has invalid work_shard")
                    blocked_by = list(task.get("blockedBy", []))
                    metadata = task.get("metadata") or {}
                    card_selector = task.get("card_selector") or task.get(
                        "cardSelector"
                    )

                    await conn.execute(
                        """
                        INSERT INTO tasks
                            (id, subject, description, thread_id, owner, status,
                             blocked_by_count, metadata, task_type, work_shard,
                             card_selector, current_execution_id, current_attempt,
                             claimed_at, lease_expires_at, last_heartbeat)
                        VALUES
                            (%s, %s, %s, %s, %s, 'pending', %s, %s::jsonb,
                             %s, %s, %s::jsonb, NULL, NULL, NULL, NULL, NULL)
                        ON CONFLICT (id) DO UPDATE
                        SET subject = EXCLUDED.subject,
                            description = EXCLUDED.description,
                            thread_id = EXCLUDED.thread_id,
                            owner = EXCLUDED.owner,
                            status = 'pending',
                            blocked_by_count = EXCLUDED.blocked_by_count,
                            metadata = EXCLUDED.metadata,
                            task_type = EXCLUDED.task_type,
                            work_shard = EXCLUDED.work_shard,
                            card_selector = EXCLUDED.card_selector,
                            current_execution_id = NULL,
                            current_attempt = NULL,
                            claimed_at = NULL,
                            lease_expires_at = NULL,
                            last_heartbeat = NULL,
                            updated_at = NOW()
                        """,
                        [
                            task_id,
                            task["subject"],
                            task.get("description"),
                            thread_id,
                            owner,
                            len(blocked_by),
                            json.dumps(metadata),
                            task_type,
                            work_shard,
                            json.dumps(card_selector) if card_selector else None,
                        ],
                    )
                    tasks_inserted += 1

                    for blocker_id in blocked_by:
                        await conn.execute(
                            """
                            INSERT INTO task_dependencies (task_id, blocker_id)
                            VALUES (%s, %s)
                            ON CONFLICT (task_id, blocker_id) DO NOTHING
                            """,
                            [task_id, blocker_id],
                        )
                        dependencies_inserted += 1

                    await self._enqueue_event(
                        conn,
                        sender="lead",
                        target=None,
                        event_type="task.published",
                        payload={
                            "task_type": task_type,
                            "work_shard": work_shard,
                        },
                        thread_id=thread_id,
                        task_id=task_id,
                    )
                    if not blocked_by:
                        ready_tasks.append(
                            {
                                "id": task_id,
                                "task_type": task_type,
                                "work_shard": work_shard,
                            }
                        )

                for task in ready_tasks:
                    await self._enqueue_task_available(
                        conn,
                        thread_id=thread_id,
                        task_id=task["id"],
                        task_type=task["task_type"],
                        work_shard=task["work_shard"],
                    )

                return {
                    "tasks_inserted": tasks_inserted,
                    "dependencies_inserted": dependencies_inserted,
                    "plan_summary": dag_data.get("plan_summary", ""),
                }

    async def claim_next_available_task(
        self,
        thread_id: str,
        runtime_id: str,
        task_type: str,
        work_shard: int,
    ) -> ClaimedExecution | None:
        """Claim exactly one ready task and freeze its Agent Card snapshot."""
        async with self.pool.connection() as conn:
            async with conn.transaction():
                await conn.execute("SET CONSTRAINTS ALL DEFERRED")
                runtime_cursor = await conn.execute(
                    """
                    SELECT status
                    FROM agent_runtimes
                    WHERE runtime_id = %s
                    FOR UPDATE
                    """,
                    [runtime_id],
                )
                runtime = _row_to_dict(runtime_cursor, await runtime_cursor.fetchone())
                if runtime is None:
                    raise ValueError(f"runtime '{runtime_id}' is not registered")
                if runtime["status"] != "idle":
                    return None

                task_cursor = await conn.execute(
                    """
                    SELECT id, subject, description, thread_id, metadata, task_type,
                           work_shard, card_selector, current_execution_id,
                           current_attempt
                    FROM tasks
                    WHERE thread_id = %s
                      AND task_type = %s
                      AND work_shard = %s
                      AND status = 'pending'
                      AND blocked_by_count = 0
                    ORDER BY
                        CASE WHEN metadata->>'priority' IS NOT NULL
                             THEN (metadata->>'priority')::int
                             ELSE 5
                        END,
                        updated_at,
                        id
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                    """,
                    [thread_id, task_type, work_shard],
                )
                task = _row_to_dict(task_cursor, await task_cursor.fetchone())
                if task is None:
                    return None

                prior_execution_id = task["current_execution_id"]
                prior_attempt = task["current_attempt"]
                if prior_execution_id is None:
                    execution_id = str(uuid4())
                    attempt = 1
                    card = await self._resolve_active_card(conn, task)
                else:
                    execution_id = str(prior_execution_id)
                    attempt = int(prior_attempt) + 1
                    card = await self._load_frozen_card(
                        conn,
                        task_id=task["id"],
                        execution_id=execution_id,
                        attempt=int(prior_attempt),
                    )

                if card is None:
                    await conn.execute(
                        """
                        UPDATE tasks
                        SET status = 'failed', owner = NULL, updated_at = NOW(),
                            metadata = COALESCE(metadata, '{}'::jsonb)
                                || jsonb_build_object(
                                    'last_error', 'card_resolution_failed'
                                )
                        WHERE id = %s
                        """,
                        [task["id"]],
                    )
                    return None

                await conn.execute(
                    """
                    INSERT INTO task_executions
                        (execution_id, attempt, task_id, thread_id, runtime_id,
                         agent_card_id, agent_card_version, agent_card_digest,
                         agent_card_config_hash, status, lease_expires_at,
                         last_heartbeat_at)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'claimed',
                         NOW() + (%s * INTERVAL '1 second'), NOW())
                    """,
                    [
                        execution_id,
                        attempt,
                        task["id"],
                        task["thread_id"],
                        runtime_id,
                        card["agent_card_id"],
                        card["version"],
                        card["bundle_digest"],
                        card["config_hash"],
                        self.lease_duration,
                    ],
                )
                await conn.execute(
                    """
                    UPDATE tasks
                    SET owner = %s,
                        status = 'in_progress',
                        claimed_at = NOW(),
                        lease_expires_at = NOW() + (%s * INTERVAL '1 second'),
                        last_heartbeat = NOW(),
                        current_execution_id = %s,
                        current_attempt = %s,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    [
                        runtime_id,
                        self.lease_duration,
                        execution_id,
                        attempt,
                        task["id"],
                    ],
                )
                cursor = await conn.execute(
                    """
                    UPDATE agent_runtimes
                    SET status = 'loading',
                        current_execution_id = %s,
                        current_attempt = %s,
                        last_heartbeat = NOW(),
                        lease_expires_at = NOW() + (%s * INTERVAL '1 second')
                    WHERE runtime_id = %s
                    """,
                    [execution_id, attempt, self.lease_duration, runtime_id],
                )
                await self._record_execution_event(
                    conn,
                    sender=runtime_id,
                    event_type="execution.claimed",
                    task_id=task["id"],
                    execution_id=execution_id,
                    attempt=attempt,
                    runtime_id=runtime_id,
                    payload={
                        "task_type": task["task_type"],
                        "work_shard": task["work_shard"],
                    },
                )
                await self._record_execution_event(
                    conn,
                    sender=runtime_id,
                    event_type="card.resolved",
                    task_id=task["id"],
                    execution_id=execution_id,
                    attempt=attempt,
                    runtime_id=runtime_id,
                    payload={
                        "agent_card_id": card["agent_card_id"],
                        "version": card["version"],
                        "bundle_digest": card["bundle_digest"],
                        "config_hash": card["config_hash"],
                    },
                )
                return ClaimedExecution(
                    task_id=task["id"],
                    subject=task["subject"],
                    description=task["description"],
                    thread_id=task["thread_id"],
                    task_type=task["task_type"],
                    work_shard=task["work_shard"],
                    metadata=_as_json_object(task["metadata"]),
                    execution_id=execution_id,
                    attempt=attempt,
                    runtime_id=runtime_id,
                    agent_card_id=card["agent_card_id"],
                    agent_card_version=card["version"],
                    agent_card_digest=card["bundle_digest"],
                    agent_card_config_hash=card["config_hash"],
                    card_system_prompt=card["system_prompt"],
                    card_tool_allowlist=_as_json_list(card["tool_allowlist"]),
                    card_skill_allowlist=_as_json_list(card["skill_allowlist"]),
                    card_runtime_config=_as_json_object(card["runtime_config"]),
                    card_bundle_ref=card["bundle_ref"],
                )

    async def mark_card_load_started(
        self,
        *,
        task_id: str,
        execution_id: str,
        attempt: int,
        runtime_id: str,
    ) -> bool:
        return await self._record_active_execution_event(
            task_id=task_id,
            execution_id=execution_id,
            attempt=attempt,
            runtime_id=runtime_id,
            event_type="card.load_started",
        )

    async def mark_execution_started(
        self,
        *,
        task_id: str,
        execution_id: str,
        attempt: int,
        runtime_id: str,
    ) -> bool:
        async with self.pool.connection() as conn:
            async with conn.transaction():
                if not await self._is_current_attempt(
                    conn, task_id, execution_id, attempt, runtime_id
                ):
                    await self._record_stale_update(
                        conn, task_id, execution_id, attempt, runtime_id, "start"
                    )
                    return False
                cursor = await conn.execute(
                    """
                    UPDATE task_executions
                    SET status = 'running',
                        started_at = COALESCE(started_at, NOW()),
                        last_heartbeat_at = NOW(),
                        lease_expires_at = NOW() + (%s * INTERVAL '1 second')
                    WHERE task_id = %s AND execution_id = %s AND attempt = %s
                      AND runtime_id = %s AND status = 'claimed'
                    """,
                    [
                        self.lease_duration,
                        task_id,
                        execution_id,
                        attempt,
                        runtime_id,
                    ],
                )
                if cursor.rowcount == 0:
                    return False
                await conn.execute(
                    """
                    UPDATE agent_runtimes
                    SET status = 'running',
                        last_heartbeat = NOW(),
                        lease_expires_at = NOW() + (%s * INTERVAL '1 second')
                    WHERE runtime_id = %s
                    """,
                    [self.lease_duration, runtime_id],
                )
                await self._record_execution_event(
                    conn,
                    sender=runtime_id,
                    event_type="card.loaded",
                    task_id=task_id,
                    execution_id=execution_id,
                    attempt=attempt,
                    runtime_id=runtime_id,
                )
                await self._record_execution_event(
                    conn,
                    sender=runtime_id,
                    event_type="execution.started",
                    task_id=task_id,
                    execution_id=execution_id,
                    attempt=attempt,
                    runtime_id=runtime_id,
                )
                return True

    async def renew_lease(
        self,
        *,
        task_id: str,
        execution_id: str,
        attempt: int,
        runtime_id: str,
        lease_duration: int | None = None,
    ) -> bool:
        lease_duration = lease_duration or self.lease_duration
        async with self.pool.connection() as conn:
            async with conn.transaction():
                if not await self._is_current_attempt(
                    conn, task_id, execution_id, attempt, runtime_id
                ):
                    await self._record_stale_update(
                        conn, task_id, execution_id, attempt, runtime_id, "heartbeat"
                    )
                    return False
                cursor = await conn.execute(
                    """
                    UPDATE task_executions
                    SET last_heartbeat_at = NOW(),
                        lease_expires_at = NOW() + (%s * INTERVAL '1 second')
                    WHERE task_id = %s AND execution_id = %s AND attempt = %s
                      AND runtime_id = %s
                      AND status IN ('claimed', 'running')
                    """,
                    [
                        lease_duration,
                        task_id,
                        execution_id,
                        attempt,
                        runtime_id,
                    ],
                )
                if cursor.rowcount == 0:
                    return False
                await conn.execute(
                    """
                    UPDATE tasks
                    SET lease_expires_at = NOW() + (%s * INTERVAL '1 second'),
                        last_heartbeat = NOW(), updated_at = NOW()
                    WHERE id = %s AND current_execution_id = %s
                      AND current_attempt = %s AND owner = %s
                    """,
                    [lease_duration, task_id, execution_id, attempt, runtime_id],
                )
                await conn.execute(
                    """
                    UPDATE agent_runtimes
                    SET last_heartbeat = NOW(),
                        lease_expires_at = NOW() + (%s * INTERVAL '1 second')
                    WHERE runtime_id = %s
                    """,
                    [lease_duration, runtime_id],
                )
                await self._record_execution_event(
                    conn,
                    sender=runtime_id,
                    event_type="execution.heartbeat",
                    task_id=task_id,
                    execution_id=execution_id,
                    attempt=attempt,
                    runtime_id=runtime_id,
                    publish=False,
                )
                return True

    async def complete_execution(
        self,
        *,
        task_id: str,
        execution_id: str,
        attempt: int,
        runtime_id: str,
        summary: str,
        result_path: str | None = None,
    ) -> bool:
        summary = summary[:1000]
        async with self.pool.connection() as conn:
            async with conn.transaction():
                if not await self._is_current_attempt(
                    conn, task_id, execution_id, attempt, runtime_id
                ):
                    await self._record_stale_update(
                        conn, task_id, execution_id, attempt, runtime_id, "complete"
                    )
                    return False
                cursor = await conn.execute(
                    """
                    UPDATE task_executions
                    SET status = 'completed', ended_at = NOW(),
                        result_summary = %s, result_path = %s,
                        lease_expires_at = NULL
                    WHERE task_id = %s AND execution_id = %s AND attempt = %s
                      AND runtime_id = %s AND status IN ('claimed', 'running')
                    """,
                    [
                        summary,
                        result_path,
                        task_id,
                        execution_id,
                        attempt,
                        runtime_id,
                    ],
                )
                if cursor.rowcount == 0:
                    return False
                await conn.execute(
                    """
                    UPDATE tasks
                    SET status = 'completed', owner = NULL, claimed_at = NULL,
                        lease_expires_at = NULL, last_heartbeat = NOW(),
                        updated_at = NOW(),
                        metadata = COALESCE(metadata, '{}'::jsonb)
                            || jsonb_build_object(
                                'summary', %s::text,
                                'result_path', %s::text
                            )
                    WHERE id = %s AND current_execution_id = %s
                      AND current_attempt = %s AND owner = %s
                    """,
                    [summary, result_path, task_id, execution_id, attempt, runtime_id],
                )
                await self._set_runtime_unloading(
                    conn, runtime_id, execution_id, attempt
                )
                await self._record_execution_event(
                    conn,
                    sender=runtime_id,
                    event_type="execution.completed",
                    task_id=task_id,
                    execution_id=execution_id,
                    attempt=attempt,
                    runtime_id=runtime_id,
                    payload={"summary": summary, "result_path": result_path},
                )
                await self._record_execution_event(
                    conn,
                    sender=runtime_id,
                    event_type="card.unload_started",
                    task_id=task_id,
                    execution_id=execution_id,
                    attempt=attempt,
                    runtime_id=runtime_id,
                )
                await self._update_downstream_dependencies(conn, task_id)
                return True

    async def fail_execution(
        self,
        *,
        task_id: str,
        execution_id: str,
        attempt: int,
        runtime_id: str,
        error: str,
    ) -> RetryDecision:
        error = error[:4000]
        async with self.pool.connection() as conn:
            async with conn.transaction():
                if not await self._is_current_attempt(
                    conn, task_id, execution_id, attempt, runtime_id
                ):
                    await self._record_stale_update(
                        conn, task_id, execution_id, attempt, runtime_id, "fail"
                    )
                    return RetryDecision(False, execution_id, attempt)
                cursor = await conn.execute(
                    """
                    UPDATE task_executions
                    SET status = 'failed', ended_at = NOW(), failure_reason = %s,
                        lease_expires_at = NULL
                    WHERE task_id = %s AND execution_id = %s AND attempt = %s
                      AND runtime_id = %s AND status IN ('claimed', 'running')
                    """,
                    [error, task_id, execution_id, attempt, runtime_id],
                )
                if cursor.rowcount == 0:
                    return RetryDecision(False, execution_id, attempt)

                retry_scheduled = attempt < self.max_attempts
                await conn.execute(
                    """
                    UPDATE tasks
                    SET status = %s, owner = NULL, claimed_at = NULL,
                        lease_expires_at = NULL, last_heartbeat = NOW(),
                        updated_at = NOW(),
                        metadata = COALESCE(metadata, '{}'::jsonb)
                            || jsonb_build_object('last_error', %s::text)
                    WHERE id = %s AND current_execution_id = %s
                      AND current_attempt = %s AND owner = %s
                    """,
                    [
                        "pending" if retry_scheduled else "failed",
                        error,
                        task_id,
                        execution_id,
                        attempt,
                        runtime_id,
                    ],
                )
                await self._set_runtime_unloading(
                    conn, runtime_id, execution_id, attempt
                )
                await self._record_execution_event(
                    conn,
                    sender=runtime_id,
                    event_type="execution.failed",
                    task_id=task_id,
                    execution_id=execution_id,
                    attempt=attempt,
                    runtime_id=runtime_id,
                    payload={"error": error, "retry_scheduled": retry_scheduled},
                )
                await self._record_execution_event(
                    conn,
                    sender=runtime_id,
                    event_type="card.unload_started",
                    task_id=task_id,
                    execution_id=execution_id,
                    attempt=attempt,
                    runtime_id=runtime_id,
                )
                if retry_scheduled:
                    await self._record_execution_event(
                        conn,
                        sender=runtime_id,
                        event_type="execution.retried",
                        task_id=task_id,
                        execution_id=execution_id,
                        attempt=attempt,
                        runtime_id=runtime_id,
                        payload={"next_attempt": attempt + 1},
                    )
                    task_cursor = await conn.execute(
                        """
                        SELECT thread_id, task_type, work_shard
                        FROM tasks WHERE id = %s
                        """,
                        [task_id],
                    )
                    task = _row_to_dict(task_cursor, await task_cursor.fetchone())
                    if task is not None:
                        await self._enqueue_task_available(
                            conn,
                            thread_id=task["thread_id"],
                            task_id=task_id,
                            task_type=task["task_type"],
                            work_shard=task["work_shard"],
                        )
                return RetryDecision(retry_scheduled, execution_id, attempt)

    async def cancel_execution(
        self,
        *,
        task_id: str,
        execution_id: str,
        attempt: int,
        runtime_id: str,
        reason: str,
    ) -> bool:
        """Cancel the current attempt without making it eligible for retry."""
        reason = reason[:4000]
        async with self.pool.connection() as conn:
            async with conn.transaction():
                if not await self._is_current_attempt(
                    conn, task_id, execution_id, attempt, runtime_id
                ):
                    await self._record_stale_update(
                        conn, task_id, execution_id, attempt, runtime_id, "cancel"
                    )
                    return False
                cursor = await conn.execute(
                    """
                    UPDATE task_executions
                    SET status = 'cancelled', ended_at = NOW(),
                        failure_reason = %s, lease_expires_at = NULL
                    WHERE task_id = %s AND execution_id = %s AND attempt = %s
                      AND runtime_id = %s AND status IN ('claimed', 'running')
                    """,
                    [reason, task_id, execution_id, attempt, runtime_id],
                )
                if cursor.rowcount == 0:
                    return False
                await conn.execute(
                    """
                    UPDATE tasks
                    SET status = 'cancelled', owner = NULL, claimed_at = NULL,
                        lease_expires_at = NULL, last_heartbeat = NOW(),
                        updated_at = NOW(),
                        metadata = COALESCE(metadata, '{}'::jsonb)
                            || jsonb_build_object('cancel_reason', %s::text)
                    WHERE id = %s AND current_execution_id = %s
                      AND current_attempt = %s AND owner = %s
                    """,
                    [reason, task_id, execution_id, attempt, runtime_id],
                )
                await self._set_runtime_unloading(
                    conn, runtime_id, execution_id, attempt
                )
                await self._record_execution_event(
                    conn,
                    sender=runtime_id,
                    event_type="execution.cancelled",
                    task_id=task_id,
                    execution_id=execution_id,
                    attempt=attempt,
                    runtime_id=runtime_id,
                    payload={"reason": reason},
                )
                await self._record_execution_event(
                    conn,
                    sender=runtime_id,
                    event_type="card.unload_started",
                    task_id=task_id,
                    execution_id=execution_id,
                    attempt=attempt,
                    runtime_id=runtime_id,
                )
                return True

    async def mark_card_load_failed(
        self,
        *,
        task_id: str,
        execution_id: str,
        attempt: int,
        runtime_id: str,
        error: str,
    ) -> RetryDecision:
        async with self.pool.connection() as conn:
            async with conn.transaction():
                if await self._is_current_attempt(
                    conn, task_id, execution_id, attempt, runtime_id
                ):
                    await self._record_execution_event(
                        conn,
                        sender=runtime_id,
                        event_type="card.load_failed",
                        task_id=task_id,
                        execution_id=execution_id,
                        attempt=attempt,
                        runtime_id=runtime_id,
                        payload={"error": error[:4000]},
                    )
        return await self.fail_execution(
            task_id=task_id,
            execution_id=execution_id,
            attempt=attempt,
            runtime_id=runtime_id,
            error=f"card_load_failed: {error}",
        )

    async def finish_card_unload(
        self,
        *,
        task_id: str,
        execution_id: str,
        attempt: int,
        runtime_id: str,
        error: str | None = None,
    ) -> bool:
        """Release a Runtime only after the caller discarded the loaded Card."""
        async with self.pool.connection() as conn:
            async with conn.transaction():
                cursor = await conn.execute(
                    """
                    SELECT status
                    FROM task_executions
                    WHERE task_id = %s AND execution_id = %s AND attempt = %s
                      AND runtime_id = %s
                    FOR UPDATE
                    """,
                    [task_id, execution_id, attempt, runtime_id],
                )
                execution = _row_to_dict(cursor, await cursor.fetchone())
                if execution is None or execution["status"] not in {
                    "completed",
                    "failed",
                    "expired",
                    "cancelled",
                }:
                    return False
                if error:
                    await self._record_execution_event(
                        conn,
                        sender=runtime_id,
                        event_type="card.unload_failed",
                        task_id=task_id,
                        execution_id=execution_id,
                        attempt=attempt,
                        runtime_id=runtime_id,
                        payload={"error": error[:4000]},
                    )
                    return False
                cursor = await conn.execute(
                    """
                    UPDATE agent_runtimes
                    SET status = 'idle', current_execution_id = NULL,
                        current_attempt = NULL, last_heartbeat = NOW(),
                        lease_expires_at = NOW() + (%s * INTERVAL '1 second')
                    WHERE runtime_id = %s AND current_execution_id = %s
                      AND current_attempt = %s AND status = 'unloading'
                    """,
                    [self.lease_duration, runtime_id, execution_id, attempt],
                )
                if cursor.rowcount == 0:
                    return False
                await self._record_execution_event(
                    conn,
                    sender=runtime_id,
                    event_type="card.unloaded",
                    task_id=task_id,
                    execution_id=execution_id,
                    attempt=attempt,
                    runtime_id=runtime_id,
                )
                return True

    async def reclaim_leased_tasks(self) -> int:
        """Expire old attempts and enqueue retries without trusting late updates."""
        reclaimed = 0
        async with self.pool.connection() as conn:
            async with conn.transaction():
                cursor = await conn.execute(
                    """
                    SELECT e.task_id, e.execution_id, e.attempt, e.runtime_id,
                           t.thread_id, t.task_type, t.work_shard
                    FROM task_executions e
                    JOIN tasks t ON t.id = e.task_id
                    WHERE e.status IN ('claimed', 'running')
                      AND e.lease_expires_at < NOW()
                      AND t.current_execution_id = e.execution_id
                      AND t.current_attempt = e.attempt
                    FOR UPDATE OF e, t SKIP LOCKED
                    """
                )
                expired = [_row_to_dict(cursor, row) for row in await cursor.fetchall()]
                for row in expired:
                    await conn.execute(
                        """
                        UPDATE task_executions
                        SET status = 'expired', ended_at = NOW(),
                            failure_reason = 'lease_expired',
                            lease_expires_at = NULL
                        WHERE task_id = %s AND execution_id = %s AND attempt = %s
                        """,
                        [row["task_id"], row["execution_id"], row["attempt"]],
                    )
                    retry_scheduled = row["attempt"] < self.max_attempts
                    await conn.execute(
                        """
                        UPDATE tasks
                        SET status = %s, owner = NULL, claimed_at = NULL,
                            lease_expires_at = NULL, updated_at = NOW(),
                            metadata = COALESCE(metadata, '{}'::jsonb)
                                || jsonb_build_object('last_error', 'lease_expired')
                        WHERE id = %s AND current_execution_id = %s
                          AND current_attempt = %s
                        """,
                        [
                            "pending" if retry_scheduled else "failed",
                            row["task_id"],
                            row["execution_id"],
                            row["attempt"],
                        ],
                    )
                    if row["runtime_id"]:
                        await conn.execute(
                            """
                            UPDATE agent_runtimes
                            SET status = 'idle', current_execution_id = NULL,
                                current_attempt = NULL, last_heartbeat = NOW(),
                                lease_expires_at = NOW() + (%s * INTERVAL '1 second')
                            WHERE runtime_id = %s AND current_execution_id = %s
                              AND current_attempt = %s
                            """,
                            [
                                self.lease_duration,
                                row["runtime_id"],
                                row["execution_id"],
                                row["attempt"],
                            ],
                        )
                    await self._record_execution_event(
                        conn,
                        sender="scheduler",
                        event_type="execution.expired",
                        task_id=row["task_id"],
                        execution_id=str(row["execution_id"]),
                        attempt=row["attempt"],
                        runtime_id=row["runtime_id"],
                        payload={"retry_scheduled": retry_scheduled},
                    )
                    await self._record_execution_event(
                        conn,
                        sender="scheduler",
                        event_type="card.unload_started",
                        task_id=row["task_id"],
                        execution_id=str(row["execution_id"]),
                        attempt=row["attempt"],
                        runtime_id=row["runtime_id"],
                        payload={"reason": "runtime_lease_expired"},
                    )
                    await self._record_execution_event(
                        conn,
                        sender="scheduler",
                        event_type="card.unloaded",
                        task_id=row["task_id"],
                        execution_id=str(row["execution_id"]),
                        attempt=row["attempt"],
                        runtime_id=row["runtime_id"],
                        payload={"reason": "runtime_lease_expired"},
                    )
                    if retry_scheduled:
                        await self._enqueue_task_available(
                            conn,
                            thread_id=row["thread_id"],
                            task_id=row["task_id"],
                            task_type=row["task_type"],
                            work_shard=row["work_shard"],
                        )
                    reclaimed += 1
        return reclaimed

    async def get_ready_shards(
        self,
        *,
        thread_id: str | None = None,
        limit: int = 256,
    ) -> list[dict[str, Any]]:
        """Return ready shards for periodic runtime compensation scans."""
        filters = ["status = 'pending'", "blocked_by_count = 0"]
        params: list[Any] = []
        if thread_id is not None:
            filters.append("thread_id = %s")
            params.append(thread_id)
        params.append(limit)
        async with self.pool.connection() as conn:
            cursor = await conn.execute(
                f"""
                SELECT DISTINCT thread_id, task_type, work_shard
                FROM tasks
                WHERE {' AND '.join(filters)}
                ORDER BY thread_id, task_type, work_shard
                LIMIT %s
                """,
                params,
            )
            return [_row_to_dict(cursor, row) for row in await cursor.fetchall()]

    async def get_task_execution_history(self, task_id: str) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            cursor = await conn.execute(
                """
                SELECT execution_id, attempt, task_id, thread_id, runtime_id,
                       agent_card_id, agent_card_version, agent_card_digest,
                       agent_card_config_hash, status, claimed_at, started_at,
                       ended_at, lease_expires_at, last_heartbeat_at,
                       result_summary, result_path, failure_reason
                FROM task_executions
                WHERE task_id = %s
                ORDER BY claimed_at, attempt
                """,
                [task_id],
            )
            return [_row_to_dict(cursor, row) for row in await cursor.fetchall()]

    async def get_execution_events(
        self,
        execution_id: str,
        attempt: int,
    ) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            cursor = await conn.execute(
                """
                SELECT task_id, execution_id, attempt, event_id, correlation_id,
                       runtime_id, event_type, payload, created_at, sequence_no
                FROM task_execution_events
                WHERE execution_id = %s AND attempt = %s
                ORDER BY sequence_no
                """,
                [execution_id, attempt],
            )
            return [_row_to_dict(cursor, row) for row in await cursor.fetchall()]

    async def _resolve_active_card(
        self,
        conn: Any,
        task: dict[str, Any],
    ) -> dict[str, Any] | None:
        selector = _as_json_object(task["card_selector"])
        if selector.get("agent_card_id") and selector.get("version"):
            cursor = await conn.execute(
                """
                SELECT agent_card_id, version, system_prompt, tool_allowlist,
                       skill_allowlist, runtime_config, bundle_ref,
                       bundle_digest, config_hash
                FROM agent_cards
                WHERE agent_card_id = %s AND version = %s AND status = 'active'
                """,
                [selector["agent_card_id"], selector["version"]],
            )
            return _row_to_dict(cursor, await cursor.fetchone())

        cursor = await conn.execute(
            """
            SELECT agent_card_id, version, system_prompt, tool_allowlist,
                   skill_allowlist, runtime_config, bundle_ref, bundle_digest,
                   config_hash
            FROM agent_cards
            WHERE status = 'active'
              AND task_types @> %s::jsonb
            ORDER BY created_at DESC
            LIMIT 2
            """,
            [json.dumps([task["task_type"]])],
        )
        rows = [_row_to_dict(cursor, row) for row in await cursor.fetchall()]
        return rows[0] if len(rows) == 1 else None

    async def _load_frozen_card(
        self,
        conn: Any,
        *,
        task_id: str,
        execution_id: str,
        attempt: int,
    ) -> dict[str, Any] | None:
        cursor = await conn.execute(
            """
            SELECT c.agent_card_id, c.version, c.system_prompt, c.tool_allowlist,
                   c.skill_allowlist, c.runtime_config, c.bundle_ref,
                   e.agent_card_digest AS bundle_digest,
                   e.agent_card_config_hash AS config_hash,
                   c.bundle_digest AS current_bundle_digest,
                   c.config_hash AS current_config_hash
            FROM task_executions e
            JOIN agent_cards c
              ON c.agent_card_id = e.agent_card_id
             AND c.version = e.agent_card_version
            WHERE e.task_id = %s AND e.execution_id = %s AND e.attempt = %s
              AND c.status = 'active'
            """,
            [task_id, execution_id, attempt],
        )
        card = _row_to_dict(cursor, await cursor.fetchone())
        if card is None:
            return None
        if (
            card["bundle_digest"] != card["current_bundle_digest"]
            or card["config_hash"] != card["current_config_hash"]
        ):
            logger.error(
                "Frozen Agent Card snapshot no longer matches registry",
                extra={
                    "task_id": task_id,
                    "execution_id": execution_id,
                    "attempt": attempt,
                    "agent_card_id": card["agent_card_id"],
                    "agent_card_version": card["version"],
                },
            )
            return None
        card.pop("current_bundle_digest", None)
        card.pop("current_config_hash", None)
        return card

    async def _is_current_attempt(
        self,
        conn: Any,
        task_id: str,
        execution_id: str,
        attempt: int,
        runtime_id: str,
    ) -> bool:
        cursor = await conn.execute(
            """
            SELECT 1
            FROM tasks t
            JOIN task_executions e
              ON e.task_id = t.id
             AND e.execution_id = t.current_execution_id
             AND e.attempt = t.current_attempt
            WHERE t.id = %s
              AND t.current_execution_id = %s
              AND t.current_attempt = %s
              AND t.owner = %s
              AND e.runtime_id = %s
              AND e.status IN ('claimed', 'running')
            """,
            [task_id, execution_id, attempt, runtime_id, runtime_id],
        )
        return await cursor.fetchone() is not None

    async def _record_stale_update(
        self,
        conn: Any,
        task_id: str,
        execution_id: str,
        attempt: int,
        runtime_id: str,
        operation: str,
    ) -> None:
        await self._record_execution_event(
            conn,
            sender=runtime_id,
            event_type="execution.stale_update_rejected",
            task_id=task_id,
            execution_id=execution_id,
            attempt=attempt,
            runtime_id=runtime_id,
            payload={"operation": operation},
            publish=False,
        )

    async def _record_active_execution_event(
        self,
        *,
        task_id: str,
        execution_id: str,
        attempt: int,
        runtime_id: str,
        event_type: str,
    ) -> bool:
        async with self.pool.connection() as conn:
            async with conn.transaction():
                if not await self._is_current_attempt(
                    conn, task_id, execution_id, attempt, runtime_id
                ):
                    await self._record_stale_update(
                        conn,
                        task_id,
                        execution_id,
                        attempt,
                        runtime_id,
                        event_type,
                    )
                    return False
                await self._record_execution_event(
                    conn,
                    sender=runtime_id,
                    event_type=event_type,
                    task_id=task_id,
                    execution_id=execution_id,
                    attempt=attempt,
                    runtime_id=runtime_id,
                )
                return True

    async def _record_execution_event(
        self,
        conn: Any,
        *,
        sender: str,
        event_type: str,
        task_id: str,
        execution_id: str,
        attempt: int,
        runtime_id: str | None,
        payload: dict[str, Any] | None = None,
        publish: bool = True,
    ) -> str:
        envelope = build_envelope(
            sender=sender,
            target=None,
            event_type=event_type,
            payload=payload or {},
            task_id=task_id,
            execution_id=execution_id,
            attempt=attempt,
        )
        await conn.execute(
            """
            INSERT INTO task_execution_events
                (task_id, execution_id, attempt, event_id, correlation_id,
                 runtime_id, event_type, payload)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            [
                task_id,
                execution_id,
                attempt,
                envelope.event_id,
                envelope.correlation_id,
                runtime_id,
                event_type,
                json.dumps(envelope.payload),
            ],
        )
        if publish:
            await enqueue_outbox_event(conn, envelope)
        return envelope.event_id

    async def _enqueue_event(
        self,
        conn: Any,
        *,
        sender: str,
        target: str | None,
        event_type: str,
        payload: dict[str, Any],
        thread_id: str | None = None,
        task_id: str | None = None,
        execution_id: str | None = None,
        attempt: int | None = None,
    ) -> str:
        envelope = build_envelope(
            sender=sender,
            target=target,
            event_type=event_type,
            payload=payload,
            thread_id=thread_id,
            task_id=task_id,
            execution_id=execution_id,
            attempt=attempt,
        )
        return await enqueue_outbox_event(conn, envelope)

    async def _enqueue_task_available(
        self,
        conn: Any,
        *,
        thread_id: str | None,
        task_id: str,
        task_type: str,
        work_shard: int,
    ) -> str:
        return await self._enqueue_event(
            conn,
            sender="scheduler",
            target=None,
            event_type="task_available",
            payload={
                "task_type": task_type,
                "work_shard": work_shard,
            },
            thread_id=thread_id,
            task_id=task_id,
        )

    async def _set_runtime_unloading(
        self,
        conn: Any,
        runtime_id: str,
        execution_id: str,
        attempt: int,
    ) -> None:
        await conn.execute(
            """
            UPDATE agent_runtimes
            SET status = 'unloading',
                last_heartbeat = NOW(),
                lease_expires_at = NOW() + (%s * INTERVAL '1 second')
            WHERE runtime_id = %s
              AND current_execution_id = %s
              AND current_attempt = %s
            """,
            [self.lease_duration, runtime_id, execution_id, attempt],
        )

    async def _update_downstream_dependencies(self, conn: Any, task_id: str) -> None:
        cursor = await conn.execute(
            """
            SELECT d.task_id, t.thread_id, t.task_type, t.work_shard
            FROM task_dependencies d
            JOIN tasks t ON t.id = d.task_id
            WHERE d.blocker_id = %s
            FOR UPDATE OF t
            """,
            [task_id],
        )
        dependents = [_row_to_dict(cursor, row) for row in await cursor.fetchall()]
        for dependent in dependents:
            count_cursor = await conn.execute(
                """
                SELECT COUNT(*) AS remaining
                FROM task_dependencies d
                JOIN tasks blocker ON blocker.id = d.blocker_id
                WHERE d.task_id = %s AND blocker.status <> 'completed'
                """,
                [dependent["task_id"]],
            )
            count_row = _row_to_dict(count_cursor, await count_cursor.fetchone())
            remaining = int(count_row["remaining"])
            await conn.execute(
                """
                UPDATE tasks
                SET blocked_by_count = %s, updated_at = NOW()
                WHERE id = %s
                """,
                [remaining, dependent["task_id"]],
            )
            if remaining == 0:
                await self._enqueue_task_available(
                    conn,
                    thread_id=dependent["thread_id"],
                    task_id=dependent["task_id"],
                    task_type=dependent["task_type"],
                    work_shard=dependent["work_shard"],
                )
