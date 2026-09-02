from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import pytest
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from lib.db import get_postgres_uri
from lib.message_hub import AGENT_MESSAGE_TOPIC, OutboxDispatcher, RocketMQMessageBus


pytestmark = pytest.mark.skipif(
    os.getenv("LANGCODE_RUN_MQ_TESTS") != "1",
    reason="set LANGCODE_RUN_MQ_TESTS=1 with local PostgreSQL and RocketMQ",
)


def test_outbox_publish_and_directed_consumer_ack() -> None:
    async def scenario() -> None:
        pool = AsyncConnectionPool(
            get_postgres_uri(),
            min_size=1,
            max_size=4,
            kwargs={"autocommit": True, "row_factory": dict_row},
            open=False,
        )
        await pool.open()
        bus = RocketMQMessageBus(pool, receipt_timeout=30)
        event_id: str | None = None
        consumer = f"mq-test-{uuid4().hex}"
        target = f"runtime-{uuid4().hex}"
        try:
            await bus.setup()
            await bus.subscribe(
                consumer,
                topic=AGENT_MESSAGE_TOPIC,
                tag_expression="mq.integration",
                target=target,
                allow_broadcast=False,
                group_id=f"GID-{consumer}",
            )
            event_id = await bus.send(
                sender="integration-test",
                target=target,
                event_type="mq.integration",
                payload={"kind": "outbox-direct"},
            )
            dispatcher = OutboxDispatcher(pool, bus, batch_size=100)

            for _ in range(20):
                await dispatcher.dispatch_once()
                async with pool.connection() as conn:
                    cursor = await conn.execute(
                        "SELECT status FROM message_outbox WHERE event_id = %s",
                        [event_id],
                    )
                    row = await cursor.fetchone()
                if row and row["status"] == "published":
                    break
                await asyncio.sleep(0.5)
            else:
                raise AssertionError("Outbox record was not published")

            received = []
            for _ in range(40):
                received = await bus.receive(consumer, timeout=1)
                if received:
                    break
            assert [message.event_id for message in received] == [event_id]
            await bus.ack(event_id)

            async with pool.connection() as conn:
                cursor = await conn.execute(
                    """
                    SELECT result, consumed_at
                    FROM message_consumer_inbox
                    WHERE consumer_name = %s AND event_id = %s
                    """,
                    [consumer, event_id],
                )
                inbox = await cursor.fetchone()
            assert inbox is not None
            assert inbox["result"] == "consumed"
            assert inbox["consumed_at"] is not None
        finally:
            await bus.close()
            if event_id is not None:
                async with pool.connection() as conn:
                    await conn.execute(
                        "DELETE FROM message_consumer_inbox WHERE event_id = %s",
                        [event_id],
                    )
                    await conn.execute(
                        "DELETE FROM message_delivery_audit WHERE event_id = %s",
                        [event_id],
                    )
                    await conn.execute(
                        "DELETE FROM message_outbox WHERE event_id = %s",
                        [event_id],
                    )
            await pool.close()

    asyncio.run(scenario())
