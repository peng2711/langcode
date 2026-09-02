"""Message bus, PostgreSQL outbox, and RocketMQ transport primitives.

PostgreSQL remains the source of truth. ``send`` only writes a durable outbox
record; ``OutboxDispatcher`` delivers those records to RocketMQ asynchronously.
The legacy ``agent_messages`` table is intentionally not written here.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol
from uuid import uuid4

from psycopg_pool import AsyncConnectionPool

logger = logging.getLogger(__name__)

AGENT_COMMAND_TOPIC = "agent-command"
TASK_EVENT_TOPIC = "task-event"
PERMISSION_EVENT_TOPIC = "permission-event"
AGENT_MESSAGE_TOPIC = "agent-message"


@dataclass(frozen=True)
class MessageEnvelope:
    """Versioned application-level message carried by RocketMQ."""

    event_id: str
    event_type: str
    sender: str
    target: str | None
    payload: dict[str, Any]
    thread_id: str | None = None
    task_id: str | None = None
    execution_id: str | None = None
    attempt: int | None = None
    request_id: str | None = None
    correlation_id: str | None = None
    created_at: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "thread_id": self.thread_id,
            "task_id": self.task_id,
            "execution_id": self.execution_id,
            "attempt": self.attempt,
            "sender": self.sender,
            "target": self.target,
            "request_id": self.request_id,
            "correlation_id": self.correlation_id,
            "payload": self.payload,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MessageEnvelope":
        return cls(
            event_id=str(data["event_id"]),
            event_type=str(data["event_type"]),
            sender=str(data["sender"]),
            target=data.get("target"),
            payload=dict(data.get("payload") or {}),
            thread_id=data.get("thread_id"),
            task_id=data.get("task_id"),
            execution_id=data.get("execution_id"),
            attempt=data.get("attempt"),
            request_id=data.get("request_id"),
            correlation_id=data.get("correlation_id"),
            created_at=data.get("created_at"),
        )


class MessageBus(Protocol):
    """Application message bus contract used by lead agents and runtimes."""

    async def send(
        self,
        *,
        sender: str,
        target: str | None,
        event_type: str,
        payload: dict[str, Any],
        thread_id: str | None = None,
        task_id: str | None = None,
        execution_id: str | None = None,
        attempt: int | None = None,
        request_id: str | None = None,
        correlation_id: str | None = None,
    ) -> str:
        """Persist a message for asynchronous delivery and return its event id."""

    async def receive(
        self,
        consumer: str,
        timeout: float = 5,
    ) -> list[MessageEnvelope]:
        """Receive one or more messages for a configured consumer."""

    async def ack(self, event_id: str) -> None:
        """Acknowledge a successfully handled message."""

    async def retry(self, event_id: str, reason: str) -> None:
        """Request redelivery of a failed message."""


def _validate_execution_context(
    execution_id: str | None,
    attempt: int | None,
) -> None:
    if (execution_id is None) != (attempt is None):
        raise ValueError("execution_id and attempt must be provided together")
    if attempt is not None and attempt < 1:
        raise ValueError("attempt must be >= 1")


def build_envelope(
    *,
    sender: str,
    target: str | None,
    event_type: str,
    payload: dict[str, Any],
    thread_id: str | None = None,
    task_id: str | None = None,
    execution_id: str | None = None,
    attempt: int | None = None,
    request_id: str | None = None,
    correlation_id: str | None = None,
    event_id: str | None = None,
) -> MessageEnvelope:
    """Build and validate a transport-independent message envelope."""
    if not sender:
        raise ValueError("sender is required")
    if not event_type:
        raise ValueError("event_type is required")
    if not isinstance(payload, dict):
        raise TypeError("payload must be a dict")
    _validate_execution_context(execution_id, attempt)
    return MessageEnvelope(
        event_id=event_id or str(uuid4()),
        event_type=event_type,
        sender=sender,
        target=target,
        payload=payload,
        thread_id=thread_id,
        task_id=task_id,
        execution_id=execution_id,
        attempt=attempt,
        request_id=request_id,
        correlation_id=correlation_id or str(uuid4()),
        created_at=datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z"),
    )


def route_envelope(envelope: MessageEnvelope) -> tuple[str, str, str | None]:
    """Choose the RocketMQ topic, tag, and ordered message key."""
    event_type = envelope.event_type

    if event_type == "task_available":
        topic = AGENT_COMMAND_TOPIC
        tag = "task_available"
    elif event_type in {"permission_response", "execution_cancel"}:
        topic = AGENT_COMMAND_TOPIC
        tag = event_type
    elif event_type == "runtime_wakeup":
        topic = AGENT_COMMAND_TOPIC
        tag = event_type
    elif event_type.startswith("permission."):
        topic = PERMISSION_EVENT_TOPIC
        tag = event_type
    elif event_type.startswith(("task.", "execution.", "card.")):
        topic = TASK_EVENT_TOPIC
        tag = event_type
    else:
        topic = AGENT_MESSAGE_TOPIC
        tag = event_type

    message_key = (
        envelope.task_id
        or envelope.execution_id
        or envelope.request_id
        or envelope.target
        or envelope.event_id
    )
    return topic, tag, message_key


async def enqueue_outbox_event(conn: Any, envelope: MessageEnvelope) -> str:
    """Insert a message into ``message_outbox`` inside the caller's transaction."""
    topic, tag, message_key = route_envelope(envelope)
    await conn.execute(
        """
        INSERT INTO message_outbox
            (event_id, topic, tag, message_key, envelope, status, next_retry_at)
        VALUES (%s, %s, %s, %s, %s::jsonb, 'pending', NOW())
        """,
        [
            envelope.event_id,
            topic,
            tag,
            message_key,
            json.dumps(envelope.as_dict()),
        ],
    )
    return envelope.event_id


def _row_to_dict(cursor: Any, row: Any) -> dict[str, Any] | None:
    if row is None:
        return None
    if isinstance(row, dict):
        return dict(row)
    return dict(zip([desc.name for desc in cursor.description], row))


class PostgresOutboxMessageBus:
    """Message bus writer backed by PostgreSQL's transactional Outbox."""

    def __init__(self, pool: AsyncConnectionPool):
        self.pool = pool

    async def setup(self) -> None:
        """Schema is initialized by ``DAGScheduler.setup`` from ``schema.sql``."""

    async def send(
        self,
        *,
        sender: str,
        target: str | None,
        event_type: str,
        payload: dict[str, Any],
        thread_id: str | None = None,
        task_id: str | None = None,
        execution_id: str | None = None,
        attempt: int | None = None,
        request_id: str | None = None,
        correlation_id: str | None = None,
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
            request_id=request_id,
            correlation_id=correlation_id,
        )
        async with self.pool.connection() as conn:
            async with conn.transaction():
                await enqueue_outbox_event(conn, envelope)
        return envelope.event_id

    async def receive(
        self,
        consumer: str,
        timeout: float = 5,
    ) -> list[MessageEnvelope]:
        raise RuntimeError(
            "PostgresOutboxMessageBus only writes the Outbox; use "
            "RocketMQMessageBus in processes that consume messages."
        )

    async def ack(self, event_id: str) -> None:
        raise RuntimeError("PostgresOutboxMessageBus cannot acknowledge messages")

    async def retry(self, event_id: str, reason: str) -> None:
        raise RuntimeError("PostgresOutboxMessageBus cannot retry messages")

    async def create_permission_request(
        self,
        *,
        request_id: str,
        agent_name: str,
        tool_name: str,
        command: str,
        thread_id: str | None = None,
        task_id: str | None = None,
        execution_id: str | None = None,
        attempt: int | None = None,
        correlation_id: str | None = None,
    ) -> str:
        """Audit a permission request and enqueue its event atomically."""
        _validate_execution_context(execution_id, attempt)
        envelope = build_envelope(
            sender=agent_name,
            target="lead",
            event_type="permission.requested",
            payload={
                "request_id": request_id,
                "tool": tool_name,
                "command": command,
            },
            thread_id=thread_id,
            task_id=task_id,
            execution_id=execution_id,
            attempt=attempt,
            request_id=request_id,
            correlation_id=correlation_id,
        )
        async with self.pool.connection() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO permission_audit_log
                        (request_id, agent_name, tool_name, command, thread_id,
                         task_id, execution_id, attempt, event_id, correlation_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    [
                        request_id,
                        agent_name,
                        tool_name,
                        command,
                        thread_id,
                        task_id,
                        execution_id,
                        attempt,
                        envelope.event_id,
                        envelope.correlation_id,
                    ],
                )
                await enqueue_outbox_event(conn, envelope)
        return envelope.event_id

    async def decide_permission(
        self,
        *,
        request_id: str,
        decision: str,
        reason: str | None,
        decided_by: str,
        sender: str = "lead",
    ) -> str | None:
        """Persist a permission decision and send a directed response atomically."""
        if decision not in {"approved", "rejected"}:
            raise ValueError("decision must be approved or rejected")

        async with self.pool.connection() as conn:
            async with conn.transaction():
                cursor = await conn.execute(
                    """
                    SELECT request_id, agent_name, thread_id, task_id, execution_id,
                           attempt, correlation_id
                    FROM permission_audit_log
                    WHERE request_id = %s
                    FOR UPDATE
                    """,
                    [request_id],
                )
                request = _row_to_dict(cursor, await cursor.fetchone())
                if request is None:
                    return None

                envelope = build_envelope(
                    sender=sender,
                    target=request["agent_name"],
                    event_type="permission_response",
                    payload={
                        "request_id": request_id,
                        "decision": decision,
                        "reason": reason,
                    },
                    thread_id=request["thread_id"],
                    task_id=request["task_id"],
                    execution_id=(
                        str(request["execution_id"])
                        if request["execution_id"] is not None
                        else None
                    ),
                    attempt=request["attempt"],
                    request_id=request_id,
                    correlation_id=(
                        str(request["correlation_id"])
                        if request["correlation_id"] is not None
                        else None
                    ),
                )
                await conn.execute(
                    """
                    UPDATE permission_audit_log
                    SET decision = %s, reason = %s, decided_by = %s,
                        decided_at = NOW(), event_id = %s
                    WHERE request_id = %s
                    """,
                    [decision, reason, decided_by, envelope.event_id, request_id],
                )
                await enqueue_outbox_event(conn, envelope)
                return envelope.event_id

    async def get_pending_permissions(self) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            cursor = await conn.execute(
                """
                SELECT request_id, agent_name, tool_name, command, thread_id,
                       task_id, execution_id, attempt, created_at
                FROM permission_audit_log
                WHERE decision IS NULL
                ORDER BY created_at ASC
                """
            )
            rows = await cursor.fetchall()
            return [_row_to_dict(cursor, row) for row in rows]

    async def get_permission_request(self, request_id: str) -> dict[str, Any] | None:
        async with self.pool.connection() as conn:
            cursor = await conn.execute(
                """
                SELECT request_id, agent_name, tool_name, command, thread_id,
                       task_id, execution_id, attempt, created_at
                FROM permission_audit_log
                WHERE request_id = %s
                """,
                [request_id],
            )
            return _row_to_dict(cursor, await cursor.fetchone())


@dataclass
class _Receipt:
    consumer: str
    envelope: MessageEnvelope
    decision: threading.Event
    retry_requested: bool = False


class RocketMQMessageBus(PostgresOutboxMessageBus):
    """RocketMQ-backed consumer transport with PostgreSQL Outbox writes.

    The Apache Python client is callback-based. The callback waits for the
    application to call ``ack`` or ``retry``, retaining explicit acknowledgement
    semantics at the application boundary.
    """

    def __init__(
        self,
        pool: AsyncConnectionPool,
        *,
        nameserver_address: str | None = None,
        producer_group: str | None = None,
        receipt_timeout: float = 300,
    ):
        super().__init__(pool)
        self.nameserver_address = nameserver_address or os.getenv(
            "ROCKETMQ_NAMESRV_ADDR", "127.0.0.1:9876"
        )
        self.producer_group = producer_group or os.getenv(
            "ROCKETMQ_PRODUCER_GROUP", "GID-langcode-outbox"
        )
        self.receipt_timeout = receipt_timeout
        self._producer: Any | None = None
        self._producer_lock = asyncio.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._queues: dict[str, asyncio.Queue[_Receipt]] = {}
        self._receipts: dict[str, _Receipt] = {}
        self._subscriptions: dict[tuple[str, str], Any] = {}

    async def setup(self) -> None:
        self._loop = asyncio.get_running_loop()
        await self._ensure_producer()

    async def close(self) -> None:
        consumers = list(self._subscriptions.values())
        self._subscriptions.clear()
        for consumer in consumers:
            try:
                await asyncio.to_thread(consumer.shutdown)
            except Exception:
                logger.exception("Failed to stop RocketMQ consumer")
        if self._producer is not None:
            producer = self._producer
            self._producer = None
            try:
                await asyncio.to_thread(producer.shutdown)
            except Exception:
                logger.exception("Failed to stop RocketMQ producer")

    async def subscribe(
        self,
        consumer: str,
        *,
        topic: str,
        tag_expression: str = "*",
        target: str | None = None,
        allow_broadcast: bool = True,
        group_id: str | None = None,
    ) -> None:
        """Configure a local RocketMQ consumer and destination filter.

        ``allow_broadcast`` is appropriate for lifecycle event streams. It is
        disabled for directed Runtime control and normal agent inboxes.
        """
        if self._loop is None:
            self._loop = asyncio.get_running_loop()
        key = (consumer, topic)
        if key in self._subscriptions:
            return

        try:
            from rocketmq.client import PushConsumer
        except ImportError as exc:
            raise RuntimeError(
                "RocketMQ support requires the 'rocketmq' package. "
                "Install requirements.txt before starting a runtime."
            ) from exc

        local_queue = self._queues.setdefault(consumer, asyncio.Queue())
        broker_group = group_id or f"GID-langcode-{_safe_group_component(consumer)}"

        def callback(message: Any) -> Any:
            try:
                body = message.body
                if isinstance(body, bytes):
                    body = body.decode("utf-8")
                envelope = MessageEnvelope.from_dict(json.loads(body))
                if target is not None and envelope.target != target:
                    if not (allow_broadcast and envelope.target is None):
                        return None
                if self._loop is None:
                    raise RuntimeError("RocketMQ callback has no running event loop")

                receipt = _Receipt(
                    consumer=consumer,
                    envelope=envelope,
                    decision=threading.Event(),
                )
                self._receipts[envelope.event_id] = receipt
                future = asyncio.run_coroutine_threadsafe(
                    local_queue.put(receipt),
                    self._loop,
                )
                future.result(timeout=5)
                if not receipt.decision.wait(timeout=self.receipt_timeout):
                    self._receipts.pop(envelope.event_id, None)
                    raise TimeoutError(
                        f"Timed out waiting for acknowledgement of "
                        f"{envelope.event_id}"
                    )
                if receipt.retry_requested:
                    raise RuntimeError(
                        f"Application requested retry for {envelope.event_id}"
                    )
                return None
            except Exception:
                logger.exception(
                    "RocketMQ callback failed",
                    extra={"consumer": consumer, "topic": topic},
                )
                # The installed push-client treats callback exceptions as
                # RECONSUME_LATER, which provides at-least-once delivery.
                raise

        mq_consumer = PushConsumer(broker_group)
        mq_consumer.set_namesrv_addr(self.nameserver_address)
        mq_consumer.set_instance_name(f"{consumer}-{uuid4().hex[:8]}")
        mq_consumer.subscribe(topic, callback, tag_expression)
        await asyncio.to_thread(mq_consumer.start)
        self._subscriptions[key] = mq_consumer

    async def receive(
        self,
        consumer: str,
        timeout: float = 5,
    ) -> list[MessageEnvelope]:
        queue = self._queues.get(consumer)
        if queue is None:
            raise RuntimeError(
                f"Consumer '{consumer}' is not subscribed. Call subscribe() first."
            )

        receipts: list[_Receipt] = []
        try:
            receipts.append(await asyncio.wait_for(queue.get(), timeout=timeout))
        except asyncio.TimeoutError:
            return []

        while True:
            try:
                receipts.append(queue.get_nowait())
            except asyncio.QueueEmpty:
                break

        envelopes: list[MessageEnvelope] = []
        for receipt in receipts:
            if await self._claim_delivery(receipt.consumer, receipt.envelope.event_id):
                envelopes.append(receipt.envelope)
            else:
                self._resolve_receipt(receipt.envelope.event_id, retry=False)
        return envelopes

    async def ack(self, event_id: str) -> None:
        receipt = self._get_receipt(event_id)
        async with self.pool.connection() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    UPDATE message_consumer_inbox
                    SET result = 'consumed', consumed_at = NOW()
                    WHERE consumer_name = %s AND event_id = %s
                    """,
                    [receipt.consumer, event_id],
                )
                await conn.execute(
                    """
                    INSERT INTO message_delivery_audit
                        (event_id, consumer_name, delivery_status, consumed_at)
                    VALUES (%s, %s, 'consumed', NOW())
                    """,
                    [event_id, receipt.consumer],
                )
        self._resolve_receipt(event_id, retry=False)

    async def retry(self, event_id: str, reason: str) -> None:
        receipt = self._get_receipt(event_id)
        async with self.pool.connection() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    DELETE FROM message_consumer_inbox
                    WHERE consumer_name = %s AND event_id = %s
                      AND result = 'processing'
                    """,
                    [receipt.consumer, event_id],
                )
                await conn.execute(
                    """
                    INSERT INTO message_delivery_audit
                        (event_id, consumer_name, delivery_status, error_message)
                    VALUES (%s, %s, 'retry_requested', %s)
                    """,
                    [event_id, receipt.consumer, reason[:4000]],
                )
        self._resolve_receipt(event_id, retry=True)

    async def publish_outbox_record(
        self,
        *,
        topic: str,
        tag: str,
        message_key: str | None,
        envelope: dict[str, Any],
    ) -> None:
        """Publish one already-committed Outbox record to RocketMQ."""
        await self._ensure_producer()
        try:
            from rocketmq.client import Message, SendStatus
        except ImportError as exc:
            raise RuntimeError("RocketMQ producer dependency is unavailable") from exc

        def publish() -> None:
            message = Message(topic)
            message.set_tags(tag)
            if message_key:
                message.set_keys(message_key)
            message.set_body(json.dumps(envelope, ensure_ascii=False))
            sharding_key = message_key or envelope["event_id"]
            result = self._producer.send_orderly(
                message,
                int.from_bytes(
                    sharding_key.encode("utf-8"),
                    "little",
                    signed=False,
                )
                % (2**31 - 1),
            )
            if result.status != SendStatus.OK:
                raise RuntimeError(f"RocketMQ send returned status {result.status!s}")

        await asyncio.to_thread(publish)

    async def _ensure_producer(self) -> None:
        if self._producer is not None:
            return
        async with self._producer_lock:
            if self._producer is not None:
                return
            try:
                from rocketmq.client import Producer
            except ImportError as exc:
                raise RuntimeError(
                    "RocketMQ support requires the 'rocketmq' package. "
                    "Install requirements.txt before starting LangCode."
                ) from exc
            producer = Producer(self.producer_group)
            producer.set_namesrv_addr(self.nameserver_address)
            await asyncio.to_thread(producer.start)
            self._producer = producer

    async def _claim_delivery(self, consumer: str, event_id: str) -> bool:
        async with self.pool.connection() as conn:
            async with conn.transaction():
                cursor = await conn.execute(
                    """
                    INSERT INTO message_consumer_inbox (consumer_name, event_id, result)
                    VALUES (%s, %s, 'processing')
                    ON CONFLICT (consumer_name, event_id) DO NOTHING
                    RETURNING event_id
                    """,
                    [consumer, event_id],
                )
                claimed = await cursor.fetchone() is not None
                if claimed:
                    await conn.execute(
                        """
                        INSERT INTO message_delivery_audit
                            (event_id, consumer_name, delivery_status, delivered_at)
                        VALUES (%s, %s, 'processing', NOW())
                        """,
                        [event_id, consumer],
                    )
                return claimed

    def _get_receipt(self, event_id: str) -> _Receipt:
        receipt = self._receipts.get(event_id)
        if receipt is None:
            raise KeyError(f"No active receipt for event {event_id}")
        return receipt

    def _resolve_receipt(self, event_id: str, *, retry: bool) -> None:
        receipt = self._receipts.pop(event_id, None)
        if receipt is None:
            return
        receipt.retry_requested = retry
        receipt.decision.set()


class OutboxDispatcher:
    """Publishes pending PostgreSQL Outbox records with bounded retry backoff."""

    def __init__(
        self,
        pool: AsyncConnectionPool,
        transport: RocketMQMessageBus,
        *,
        batch_size: int = 50,
        max_retries: int | None = None,
        retry_base_seconds: float = 1,
    ):
        self.pool = pool
        self.transport = transport
        self.batch_size = batch_size
        self.max_retries = max_retries or int(
            os.getenv("MESSAGE_OUTBOX_MAX_RETRIES", "12")
        )
        self.retry_base_seconds = retry_base_seconds

    async def dispatch_once(self) -> int:
        """Publish one locked batch. Returns the number of examined records."""
        processed = 0
        async with self.pool.connection() as conn:
            async with conn.transaction():
                cursor = await conn.execute(
                    """
                    SELECT event_id, topic, tag, message_key, envelope, retry_count
                    FROM message_outbox
                    WHERE status = 'pending'
                      AND (next_retry_at IS NULL OR next_retry_at <= NOW())
                    ORDER BY created_at, event_id
                    FOR UPDATE SKIP LOCKED
                    LIMIT %s
                    """,
                    [self.batch_size],
                )
                rows = await cursor.fetchall()
                for row in rows:
                    record = _row_to_dict(cursor, row)
                    processed += 1
                    try:
                        envelope = record["envelope"]
                        if isinstance(envelope, str):
                            envelope = json.loads(envelope)
                        await self.transport.publish_outbox_record(
                            topic=record["topic"],
                            tag=record["tag"],
                            message_key=record["message_key"],
                            envelope=envelope,
                        )
                        await conn.execute(
                            """
                            UPDATE message_outbox
                            SET status = 'published', published_at = NOW(),
                                last_error = NULL
                            WHERE event_id = %s
                            """,
                            [record["event_id"]],
                        )
                        await conn.execute(
                            """
                            INSERT INTO message_delivery_audit
                                (event_id, delivery_status, delivered_at, retry_count)
                            VALUES (%s, 'published', NOW(), %s)
                            """,
                            [record["event_id"], record["retry_count"]],
                        )
                    except Exception as exc:
                        retry_count = int(record["retry_count"]) + 1
                        terminal = retry_count >= self.max_retries
                        delay = self.retry_base_seconds * (2 ** min(retry_count, 10))
                        await conn.execute(
                            """
                            UPDATE message_outbox
                            SET status = %s, retry_count = %s,
                                next_retry_at = CASE
                                    WHEN %s THEN NULL
                                    ELSE NOW() + (%s * INTERVAL '1 second')
                                END,
                                last_error = %s
                            WHERE event_id = %s
                            """,
                            [
                                "failed" if terminal else "pending",
                                retry_count,
                                terminal,
                                delay,
                                str(exc)[:4000],
                                record["event_id"],
                            ],
                        )
                        await conn.execute(
                            """
                            INSERT INTO message_delivery_audit
                                (event_id, delivery_status, retry_count, error_message)
                            VALUES (%s, %s, %s, %s)
                            """,
                            [
                                record["event_id"],
                                "failed" if terminal else "retry_scheduled",
                                retry_count,
                                str(exc)[:4000],
                            ],
                        )
                        logger.exception(
                            "Outbox publish failed",
                            extra={"event_id": str(record["event_id"])},
                        )
        return processed

    async def run(self, *, interval: float = 0.5) -> None:
        while True:
            processed = await self.dispatch_once()
            if processed == 0:
                await asyncio.sleep(interval)


class InMemoryMessageBus:
    """Small MessageBus implementation for focused unit tests."""

    def __init__(self) -> None:
        self._messages: deque[MessageEnvelope] = deque()
        self._inflight: dict[str, MessageEnvelope] = {}
        self.acked: set[str] = set()
        self.retried: list[tuple[str, str]] = []

    async def send(
        self,
        *,
        sender: str,
        target: str | None,
        event_type: str,
        payload: dict[str, Any],
        thread_id: str | None = None,
        task_id: str | None = None,
        execution_id: str | None = None,
        attempt: int | None = None,
        request_id: str | None = None,
        correlation_id: str | None = None,
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
            request_id=request_id,
            correlation_id=correlation_id,
        )
        self._messages.append(envelope)
        return envelope.event_id

    async def receive(
        self,
        consumer: str,
        timeout: float = 5,
    ) -> list[MessageEnvelope]:
        del timeout
        selected: list[MessageEnvelope] = []
        remaining: deque[MessageEnvelope] = deque()
        while self._messages:
            envelope = self._messages.popleft()
            if envelope.target in {None, consumer}:
                selected.append(envelope)
                self._inflight[envelope.event_id] = envelope
            else:
                remaining.append(envelope)
        self._messages = remaining
        return selected

    async def ack(self, event_id: str) -> None:
        self._inflight.pop(event_id, None)
        self.acked.add(event_id)

    async def retry(self, event_id: str, reason: str) -> None:
        envelope = self._inflight.pop(event_id)
        self.retried.append((event_id, reason))
        self._messages.appendleft(envelope)


def _safe_group_component(value: str) -> str:
    return "".join(
        char if char.isalnum() or char in {"-", "_"} else "-" for char in value
    )
