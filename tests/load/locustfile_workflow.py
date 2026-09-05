"""End-to-end control-plane load test for the repository's DAG agent workflow.

This intentionally replaces LLM/tool execution with a configurable sleep. It
measures the repository-owned path: DAG publish, PostgreSQL notification,
message consumption, atomic claim, heartbeat, completion, dependency unlock,
and expired-lease recovery.
"""

from __future__ import annotations

import os
import random
import socket
import time
import uuid
import json
from contextlib import contextmanager
from pathlib import Path

import gevent
from gevent.lock import Semaphore
from locust import User, between, constant, events, task
from locust.runners import MasterRunner
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from lib.dag_scheduler import (
    CLAIM_NEXT_TASK_SQL,
    CREATE_DAG_TABLES_SQL,
    RECLAIM_LEASED_TASKS_SQL,
    RENEW_LEASE_SQL,
    UNBLOCK_DEPENDENTS_SQL,
)
from lib.db import get_pool_config, get_postgres_uri
from lib.message_hub import CREATE_TABLES_SQL as CREATE_MESSAGE_TABLES_SQL


RUN_ID = os.getenv("LOAD_RUN_ID", f"workflow-{int(time.time())}")
THREAD_ID = os.getenv("LOAD_THREAD_ID", RUN_ID)
SEED_DAGS = int(os.getenv("WORKFLOW_SEED_DAGS", "10000"))
DAG_WIDTH = int(os.getenv("WORKFLOW_DAG_WIDTH", "4"))
EXECUTION_MIN_MS = float(os.getenv("WORKFLOW_EXECUTION_MIN_MS", "10"))
EXECUTION_MAX_MS = float(os.getenv("WORKFLOW_EXECUTION_MAX_MS", "50"))
HEARTBEAT_EVERY = int(os.getenv("WORKFLOW_HEARTBEAT_EVERY", "20"))
INBOX_EVERY = int(os.getenv("WORKFLOW_INBOX_EVERY", "0"))
PUBLISH_INTERVAL = float(os.getenv("WORKFLOW_PUBLISH_INTERVAL", "5"))
NOTIFY_FANOUT = int(os.getenv("WORKFLOW_NOTIFY_FANOUT", "0"))
RECOVERY_INTERVAL = float(os.getenv("WORKFLOW_RECOVERY_INTERVAL", "10"))
RECOVERY_BATCH = int(os.getenv("WORKFLOW_RECOVERY_BATCH", "10"))
REPORT_DIR = Path(os.getenv("LOAD_REPORT_DIR", "reports/load"))

_pool: ConnectionPool | None = None
_agents: set[str] = set()
_agents_lock = Semaphore()


def _fire(request_type: str, name: str, started: float, *, size: int = 0, error=None) -> None:
    events.request.fire(
        request_type=request_type,
        name=name,
        response_time=(time.perf_counter() - started) * 1000,
        response_length=size,
        exception=error,
        context={"run_id": RUN_ID, "thread_id": THREAD_ID},
    )


def _validate_settings() -> None:
    if SEED_DAGS < 0:
        raise ValueError("WORKFLOW_SEED_DAGS must be >= 0")
    if DAG_WIDTH < 1:
        raise ValueError("WORKFLOW_DAG_WIDTH must be >= 1")
    if EXECUTION_MIN_MS < 0 or EXECUTION_MAX_MS < EXECUTION_MIN_MS:
        raise ValueError("invalid workflow execution latency range")
    for name, value in {
        "WORKFLOW_HEARTBEAT_EVERY": HEARTBEAT_EVERY,
        "WORKFLOW_INBOX_EVERY": INBOX_EVERY,
        "WORKFLOW_NOTIFY_FANOUT": NOTIFY_FANOUT,
        "WORKFLOW_RECOVERY_BATCH": RECOVERY_BATCH,
    }.items():
        if value < 0:
            raise ValueError(f"{name} must be >= 0")


def _create_pool() -> ConnectionPool:
    config = get_pool_config("LOAD_POSTGRES_POOL")
    return ConnectionPool(
        get_postgres_uri(),
        min_size=config.min_size,
        max_size=config.max_size,
        timeout=config.timeout,
        max_waiting=config.max_waiting,
        kwargs={"autocommit": True, "row_factory": dict_row},
        open=False,
        name=f"workflow-load-{os.getpid()}",
    )


@contextmanager
def _run_lock(conn):
    conn.execute("SELECT pg_advisory_lock(hashtextextended(%s, 0))", (RUN_ID,))
    try:
        yield
    finally:
        conn.execute("SELECT pg_advisory_unlock(hashtextextended(%s, 0))", (RUN_ID,))


def _insert_diamonds(conn, first: int, last: int, prefix: str) -> int:
    """Insert root -> parallel workers -> join DAGs using set-based SQL."""
    if last < first:
        return 0
    common = (prefix, THREAD_ID, RUN_ID, first, last)
    with conn.transaction():
        roots = conn.execute(
            """
            INSERT INTO tasks
                (id, subject, description, thread_id, status, blocked_by_count, metadata, updated_at)
            SELECT %s || '-d' || d || '-root', 'workflow root', 'root task', %s,
                   'pending', 0,
                   jsonb_build_object('priority', 5, 'load_run_id', %s::text, 'stage', 'root'), NOW()
            FROM generate_series(%s::bigint, %s::bigint) AS d
            ON CONFLICT (id) DO NOTHING
            """,
            common,
        ).rowcount
        workers = conn.execute(
            """
            INSERT INTO tasks
                (id, subject, description, thread_id, status, blocked_by_count, metadata, updated_at)
            SELECT %s || '-d' || d || '-worker-' || w, 'workflow worker', 'parallel task', %s,
                   'pending', 1,
                   jsonb_build_object('priority', 5, 'load_run_id', %s::text, 'stage', 'worker'), NOW()
            FROM generate_series(%s::bigint, %s::bigint) AS d
            CROSS JOIN generate_series(1, %s::integer) AS w
            ON CONFLICT (id) DO NOTHING
            """,
            (*common, DAG_WIDTH),
        ).rowcount
        joins = conn.execute(
            """
            INSERT INTO tasks
                (id, subject, description, thread_id, status, blocked_by_count, metadata, updated_at)
            SELECT %s || '-d' || d || '-join', 'workflow join', 'aggregate task', %s,
                   'pending', %s,
                   jsonb_build_object('priority', 5, 'load_run_id', %s::text, 'stage', 'join'), NOW()
            FROM generate_series(%s::bigint, %s::bigint) AS d
            ON CONFLICT (id) DO NOTHING
            """,
            (prefix, THREAD_ID, DAG_WIDTH, RUN_ID, first, last),
        ).rowcount
        conn.execute(
            """
            INSERT INTO task_dependencies (task_id, blocker_id)
            SELECT %s || '-d' || d || '-worker-' || w, %s || '-d' || d || '-root'
            FROM generate_series(%s::bigint, %s::bigint) AS d
            CROSS JOIN generate_series(1, %s::integer) AS w
            ON CONFLICT DO NOTHING
            """,
            (prefix, prefix, first, last, DAG_WIDTH),
        )
        conn.execute(
            """
            INSERT INTO task_dependencies (task_id, blocker_id)
            SELECT %s || '-d' || d || '-join', %s || '-d' || d || '-worker-' || w
            FROM generate_series(%s::bigint, %s::bigint) AS d
            CROSS JOIN generate_series(1, %s::integer) AS w
            ON CONFLICT DO NOTHING
            """,
            (prefix, prefix, first, last, DAG_WIDTH),
        )
    return roots + workers + joins


def _send_notification(agent_name: str) -> None:
    assert _pool is not None
    payload = json.dumps(
        {"to_agent": agent_name, "msg_type": "task_available", "thread_id": THREAD_ID}
    )
    with _pool.connection() as conn, conn.transaction():
        conn.execute(
            """
            INSERT INTO agent_messages (from_agent, to_agent, content, msg_type, thread_id)
            VALUES ('lead', %s, '{"notification":"new_tasks_available"}'::jsonb,
                    'task_available', %s)
            """,
            (agent_name, THREAD_ID),
        )
        conn.execute("SELECT pg_notify('agent_message', %s)", (payload,))


def _consume_notifications(agent_name: str) -> int:
    assert _pool is not None
    with _pool.connection() as conn:
        rows = conn.execute(
            """
            WITH claimed AS (
                SELECT id
                FROM agent_messages
                WHERE to_agent = %s AND thread_id = %s
                  AND msg_type = 'task_available' AND read_at IS NULL
                ORDER BY created_at ASC, id ASC
                FOR UPDATE SKIP LOCKED
            ), updated AS (
                UPDATE agent_messages AS m
                SET read_at = NOW()
                FROM claimed
                WHERE m.id = claimed.id
                RETURNING m.id
            )
            SELECT id FROM updated
            """,
            (agent_name, THREAD_ID),
        ).fetchall()
    return len(rows)


def _complete_task(task_id: str, owner: str) -> tuple[bool, int]:
    assert _pool is not None
    with _pool.connection() as conn, conn.transaction():
        cursor = conn.execute(
            """
            UPDATE tasks
            SET status = 'completed', updated_at = NOW(),
                metadata = jsonb_set(COALESCE(metadata, '{}'::jsonb),
                                     '{summary}', to_jsonb('load test complete'::text))
            WHERE id = %s AND owner = %s AND status = 'in_progress'
            """,
            (task_id, owner),
        )
        if cursor.rowcount == 0:
            return False, 0
        unlocked = conn.execute(UNBLOCK_DEPENDENTS_SQL, (task_id,)).fetchall()
        return True, sum(row["blocked_by_count"] == 0 for row in unlocked)


@events.test_start.add_listener
def on_test_start(environment, **_kwargs) -> None:
    global _pool
    if isinstance(environment.runner, MasterRunner):
        return
    _validate_settings()
    _pool = _create_pool()
    _pool.open(wait=True)
    with _pool.connection() as conn, _run_lock(conn):
        conn.execute(CREATE_DAG_TABLES_SQL)
        conn.execute(CREATE_MESSAGE_TABLES_SQL)
        inserted = _insert_diamonds(conn, 1, SEED_DAGS, RUN_ID)
    print(
        f"[workflow-load] run={RUN_ID} dags={SEED_DAGS} width={DAG_WIDTH} "
        f"tasks_inserted={inserted}",
        flush=True,
    )


@events.test_stop.add_listener
def on_test_stop(environment, **_kwargs) -> None:
    if isinstance(environment.runner, MasterRunner) or _pool is None:
        return
    with _pool.connection() as conn:
        status_rows = conn.execute(
            """
            SELECT status, count(*) AS count
            FROM tasks WHERE thread_id = %s GROUP BY status
            """,
            (THREAD_ID,),
        ).fetchall()
        checks = conn.execute(
            """
            SELECT
                count(*) AS total_tasks,
                count(*) FILTER (WHERE status = 'pending' AND blocked_by_count = 0) AS ready_tasks,
                count(*) FILTER (WHERE status = 'completed' AND blocked_by_count > 0) AS invalid_completed
            FROM tasks WHERE thread_id = %s
            """,
            (THREAD_ID,),
        ).fetchone()
        edge_count = conn.execute(
            """
            SELECT count(*) AS count
            FROM task_dependencies AS d
            JOIN tasks AS t ON t.id = d.task_id
            WHERE t.thread_id = %s
            """,
            (THREAD_ID,),
        ).fetchone()["count"]
        messages = conn.execute(
            """
            SELECT count(*) AS total,
                   count(*) FILTER (WHERE read_at IS NULL) AS unread
            FROM agent_messages WHERE thread_id = %s
            """,
            (THREAD_ID,),
        ).fetchone()

    summary = {
        "run_id": RUN_ID,
        "thread_id": THREAD_ID,
        "process_id": os.getpid(),
        "dag_width": DAG_WIDTH,
        "task_status": {row["status"]: row["count"] for row in status_rows},
        "total_tasks": checks["total_tasks"],
        "ready_tasks": checks["ready_tasks"],
        "invalid_completed_tasks": checks["invalid_completed"],
        "dependency_edges": edge_count,
        "messages_total": messages["total"],
        "messages_unread": messages["unread"],
    }
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORT_DIR / f"workflow_{RUN_ID}_summary_{os.getpid()}.json"
    path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"[workflow-load] summary={path} {summary}", flush=True)


class WorkflowAgentUser(User):
    """Simulated Sub Agent using the repository's database control path."""

    weight = 100
    wait_time = between(0.005, 0.02)

    def on_start(self) -> None:
        self.agent_name = f"agent-{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex}"
        self.completed = 0
        with _agents_lock:
            _agents.add(self.agent_name)

    def on_stop(self) -> None:
        with _agents_lock:
            _agents.discard(self.agent_name)

    @task
    def execute_workflow_task(self) -> None:
        assert _pool is not None
        cycle_started = time.perf_counter()
        claimed = None
        error = None

        claim_started = time.perf_counter()
        try:
            with _pool.connection() as conn:
                claimed = conn.execute(
                    CLAIM_NEXT_TASK_SQL,
                    (THREAD_ID, self.agent_name),
                ).fetchone()
        except Exception as exc:
            error = exc
        _fire(
            "DAGScheduler",
            "claim_next_task" if claimed else "claim_next_task/empty",
            claim_started,
            size=1 if claimed else 0,
            error=error,
        )
        if error is not None or claimed is None:
            if error is None:
                inbox_started = time.perf_counter()
                inbox_error = None
                consumed = 0
                try:
                    consumed = _consume_notifications(self.agent_name)
                except Exception as exc:
                    inbox_error = exc
                _fire("MessageHub", "consume_inbox", inbox_started, size=consumed, error=inbox_error)
            gevent.sleep(0.05)
            return

        gevent.sleep(random.uniform(EXECUTION_MIN_MS, EXECUTION_MAX_MS) / 1000)

        if HEARTBEAT_EVERY and (self.completed + 1) % HEARTBEAT_EVERY == 0:
            heartbeat_started = time.perf_counter()
            heartbeat_error = None
            renewed = 0
            try:
                with _pool.connection() as conn:
                    renewed = conn.execute(
                        RENEW_LEASE_SQL,
                        (60, claimed["id"], self.agent_name),
                    ).rowcount
            except Exception as exc:
                heartbeat_error = exc
            _fire("DAGScheduler", "renew_lease", heartbeat_started, size=renewed, error=heartbeat_error)

        complete_started = time.perf_counter()
        complete_error = None
        completed = 0
        try:
            did_complete, _unlocked = _complete_task(claimed["id"], self.agent_name)
            if not did_complete:
                raise RuntimeError("claimed task was not completed")
            completed = int(did_complete)
        except Exception as exc:
            complete_error = exc
        _fire("DAGScheduler", "complete_and_unlock", complete_started, size=completed, error=complete_error)
        _fire("Workflow", "claim_execute_complete", cycle_started, size=completed, error=complete_error)
        if complete_error is None:
            self.completed += 1

        if INBOX_EVERY and self.completed % INBOX_EVERY == 0:
            inbox_started = time.perf_counter()
            inbox_error = None
            consumed = 0
            try:
                consumed = _consume_notifications(self.agent_name)
            except Exception as exc:
                inbox_error = exc
            _fire("MessageHub", "consume_inbox", inbox_started, size=consumed, error=inbox_error)


class WorkflowPublisherUser(User):
    """Lead Agent publishing a DAG and notifying active Sub Agents."""

    fixed_count = 1
    wait_time = constant(PUBLISH_INTERVAL)

    @task
    def publish_dag(self) -> None:
        assert _pool is not None
        overall_started = time.perf_counter()
        started = time.perf_counter()
        error = None
        inserted = 0
        prefix = f"{RUN_ID}-live-{time.time_ns()}"
        try:
            with _pool.connection() as conn:
                inserted = _insert_diamonds(conn, 1, 1, prefix)
        except Exception as exc:
            error = exc
        _fire("LeadAgent", "publish_dag", started, size=inserted, error=error)
        if error is not None:
            _fire("LeadAgent", "publish_dag_end_to_end", overall_started, error=error)
            return

        with _agents_lock:
            agents = list(_agents)
        if NOTIFY_FANOUT:
            agents = random.sample(agents, min(NOTIFY_FANOUT, len(agents)))
        notify_started = time.perf_counter()
        notify_error = None
        sent = 0
        try:
            for agent_name in agents:
                _send_notification(agent_name)
                sent += 1
        except Exception as exc:
            notify_error = exc
        _fire("LeadAgent", "notify_agents", notify_started, size=sent, error=notify_error)
        _fire(
            "LeadAgent",
            "publish_dag_end_to_end",
            overall_started,
            size=inserted + sent,
            error=notify_error,
        )


class LeaseRecoveryUser(User):
    """The CLI's periodic expired-lease monitor, scoped to this test thread."""

    fixed_count = 1
    wait_time = constant(RECOVERY_INTERVAL)

    @task
    def reclaim_expired_leases(self) -> None:
        assert _pool is not None
        prefix = f"{RUN_ID}-crash-{time.time_ns()}"
        started = time.perf_counter()
        error = None
        reclaimed = 0
        try:
            with _pool.connection() as conn, conn.transaction():
                conn.execute(
                    """
                    INSERT INTO tasks
                        (id, subject, description, thread_id, owner, status,
                         blocked_by_count, claimed_at, lease_expires_at, metadata, updated_at)
                    SELECT %s || '-' || n, 'crashed task', 'lease recovery load task', %s,
                           'crashed-agent', 'in_progress', 0,
                           NOW() - INTERVAL '2 minutes', NOW() - INTERVAL '1 minute',
                           jsonb_build_object('priority', 5, 'load_run_id', %s::text,
                                              'retry_count', 0), NOW()
                    FROM generate_series(1, %s::integer) AS n
                    """,
                    (prefix, THREAD_ID, RUN_ID, RECOVERY_BATCH),
                )
                reclaimed = conn.execute(
                    RECLAIM_LEASED_TASKS_SQL,
                    (THREAD_ID, THREAD_ID),
                ).rowcount
        except Exception as exc:
            error = exc
        _fire("DAGScheduler", "reclaim_expired_leases", started, size=reclaimed, error=error)
