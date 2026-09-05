from __future__ import annotations

import asyncio
import json

from lib.dag_scheduler import CLAIM_NEXT_TASK_SQL
from lib.db import get_pool_config, get_postgres_uri
from lib.message_hub import AsyncPostgresMessageHub


class _AsyncContext:
    def __init__(self, value=None):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class _Cursor:
    rowcount = 2


class _Connection:
    def __init__(self):
        self.calls = []

    def transaction(self):
        return _AsyncContext()

    async def execute(self, query, params):
        self.calls.append((query, params))
        return _Cursor()


class _Pool:
    def __init__(self):
        self.conn = _Connection()
        self.connection_calls = 0

    def connection(self):
        self.connection_calls += 1
        return _AsyncContext(self.conn)


def test_postgres_uri_has_local_defaults_and_escapes_credentials(monkeypatch):
    for name in (
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
    ):
        monkeypatch.delenv(name, raising=False)

    assert get_postgres_uri() == (
        "postgresql://postgres:postgres@127.0.0.1:5432/langcode?sslmode=disable"
    )

    monkeypatch.setenv("POSTGRES_USER", "agent@example.com")
    monkeypatch.setenv("POSTGRES_PASSWORD", "p@ss/word")
    assert "agent%40example.com:p%40ss%2Fword@" in get_postgres_uri()


def test_pool_config_uses_safe_application_defaults(monkeypatch):
    for name in (
        "POSTGRES_POOL_MIN_SIZE",
        "POSTGRES_POOL_MAX_SIZE",
        "POSTGRES_POOL_TIMEOUT",
        "POSTGRES_POOL_MAX_WAITING",
    ):
        monkeypatch.delenv(name, raising=False)

    config = get_pool_config()
    assert (config.min_size, config.max_size, config.timeout, config.max_waiting) == (
        10,
        80,
        30.0,
        0,
    )


def test_claim_query_is_atomic_and_skip_locked():
    normalized = " ".join(CLAIM_NEXT_TASK_SQL.upper().split())
    assert "FOR UPDATE SKIP LOCKED" in normalized
    assert "UPDATE TASKS AS T" in normalized
    assert "RETURNING T.ID" in normalized


def test_send_many_persists_unique_inboxes_and_notifies_once():
    async def scenario():
        pool = _Pool()
        hub = AsyncPostgresMessageHub(pool)

        inserted = await hub.send_many(
            from_agent="lead",
            to_agents=["agent-1", "agent-2", "agent-1"],
            content={"notification": "new_tasks_available"},
            msg_type="task_available",
            thread_id="thread-1",
        )

        assert inserted == 2
        assert pool.connection_calls == 1
        assert len(pool.conn.calls) == 2
        assert pool.conn.calls[0][1][-1] == ["agent-1", "agent-2"]
        payload = json.loads(pool.conn.calls[1][1][1])
        assert payload == {
            "to_agent": "*",
            "msg_type": "task_available",
            "thread_id": "thread-1",
        }

    asyncio.run(scenario())


def test_send_many_with_no_recipients_skips_database():
    async def scenario():
        pool = _Pool()
        hub = AsyncPostgresMessageHub(pool)
        assert await hub.send_many("lead", [], {}, "task_available") == 0
        assert pool.connection_calls == 0

    asyncio.run(scenario())
