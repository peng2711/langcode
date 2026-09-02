"""Lead Agent tools backed by the MessageBus and execution-aware scheduler."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from langchain_core.tools import tool

from lib.dag_scheduler import DAGScheduler
from lib.message_hub import MessageBus

logger = logging.getLogger(__name__)


def create_lead_agent_tools(
    message_bus: MessageBus,
    sub_agents: dict[str, Any] | None = None,
    **_: Any,
) -> list:
    """Create lead tools without owning Runtime processes in the CLI."""
    if sub_agents is None:
        sub_agents = {}
    sub_agents.setdefault("_thread_id", None)
    pool = getattr(message_bus, "pool", None)
    if pool is None:
        raise ValueError("Lead tools require a MessageBus with a PostgreSQL pool")
    scheduler = DAGScheduler(pool=pool)

    def current_thread_id() -> str:
        return sub_agents.get("_thread_id") or "default"

    @tool
    async def spawn_sub_agent(
        name: str,
        role: str,
        task: str,
        max_rounds: int = 30,
    ) -> str:
        """Request a generic Runtime wakeup. It does not create a local worker."""
        if name in sub_agents:
            return f"Error: Runtime request '{name}' already exists"
        event_id = await message_bus.send(
            sender="lead",
            target=name,
            event_type="runtime_wakeup",
            payload={
                "requested_runtime_id": name,
                "requested_task": task,
                "max_rounds": max_rounds,
            },
            thread_id=current_thread_id(),
        )
        sub_agents[name] = {
            "name": name,
            "role": role,
            "status": "requested",
            "current_task": task,
            "event_id": event_id,
        }
        return (
            f"Runtime request '{name}' queued. Start a generic worker with "
            f"runtime id '{name}' to process task board work."
        )

    @tool
    async def assign_task(agent_name: str, task: str) -> str:
        """Send a directed non-durable work hint through MQ.

        Executable work must be published with ``publish_dag`` so it has task,
        dependency, execution, and audit records.
        """
        if agent_name not in sub_agents:
            return f"Error: Runtime request '{agent_name}' not found"
        event_id = await message_bus.send(
            sender="lead",
            target=agent_name,
            event_type="runtime_wakeup",
            payload={"requested_task": task},
            thread_id=current_thread_id(),
        )
        sub_agents[agent_name]["current_task"] = task
        sub_agents[agent_name]["event_id"] = event_id
        return (
            f"Runtime wakeup sent to '{agent_name}'. Publish executable work "
            "through publish_dag."
        )

    @tool
    def list_sub_agents() -> str:
        """List requested generic Runtimes tracked in the current lead session."""
        entries = [
            f"{name}: {info['role']} - {info['status']} - Task: {info['current_task']}"
            for name, info in sub_agents.items()
            if not name.startswith("_")
        ]
        return "\n".join(entries) if entries else "No runtime requests created yet"

    @tool
    async def shutdown_agent(agent_name: str) -> str:
        """Send a directed Runtime cancellation request via MQ."""
        if agent_name not in sub_agents:
            return f"Error: Runtime request '{agent_name}' not found"
        runtime_execution = await scheduler.get_runtime_execution(agent_name)
        execution_id = None
        attempt = None
        task_id = None
        if runtime_execution is not None:
            execution_id = runtime_execution.get("execution_id")
            attempt = runtime_execution.get("attempt")
            task_id = runtime_execution.get("task_id")
        await message_bus.send(
            sender="lead",
            target=agent_name,
            event_type="execution_cancel",
            payload={
                "reason": "shutdown requested by lead",
                "stop_runtime": True,
            },
            thread_id=current_thread_id(),
            task_id=task_id,
            execution_id=str(execution_id) if execution_id is not None else None,
            attempt=int(attempt) if attempt is not None else None,
        )
        sub_agents[agent_name]["status"] = "shutdown_requested"
        return f"Shutdown request sent to '{agent_name}'"

    @tool
    async def send_message(to: str, content: str, msg_type: str = "message") -> str:
        """Send a normal lead/runtime message through the MessageBus."""
        await message_bus.send(
            sender="lead",
            target=to,
            event_type=msg_type,
            payload={"text": content},
            thread_id=current_thread_id(),
        )
        return f"Message sent to '{to}'"

    @tool
    async def publish_dag(dag_json: str) -> str:
        """Publish a DAG and atomically enqueue its task availability events."""
        try:
            result = await scheduler.insert_dag_to_db(
                dag_data=json.loads(dag_json),
                thread_id=current_thread_id(),
            )
            return f"Published {result['tasks_inserted']} tasks to board"
        except Exception as exc:
            logger.exception("Failed to publish DAG")
            return f"Error publishing DAG: {exc}"

    @tool
    async def get_task_board_status() -> str:
        """Show task board counts for the current thread."""
        async with pool.connection() as conn:
            cursor = await conn.execute(
                """
                SELECT status, COUNT(*) AS count
                FROM tasks
                WHERE thread_id = %s
                GROUP BY status
                ORDER BY status
                """,
                [current_thread_id()],
            )
            rows = await cursor.fetchall()
        return (
            "\n".join(f"{row['status']}: {row['count']}" for row in rows)
            if rows
            else "No tasks in board"
        )

    @tool
    async def check_inbox() -> str:
        """Receive and acknowledge messages directed to the lead."""
        try:
            messages = await message_bus.receive("lead", timeout=0.1)
        except RuntimeError as exc:
            return f"Lead MQ inbox unavailable: {exc}"
        if not messages:
            return "Inbox is empty"
        lines = []
        for message in messages:
            lines.append(
                f"From: {message.sender} | Type: {message.event_type} | "
                f"Content: {message.payload}"
            )
            await message_bus.ack(message.event_id)
        return "\n".join(lines)

    @tool
    async def list_pending_permissions() -> str:
        """List permission requests awaiting a lead decision."""
        getter = getattr(message_bus, "get_pending_permissions", None)
        if getter is None:
            return "Permission audit is unavailable"
        pending = await getter()
        if not pending:
            return "No pending permission requests"
        return "\n".join(
            (
                f"[{perm['request_id']}] {perm['agent_name']}: "
                f"{perm['tool_name']} - {perm['command']}"
            )
            for perm in pending
        )

    @tool
    async def approve_permission(request_id: str, reason: str = "") -> str:
        """Approve a permission request and publish its directed MQ response."""
        decide = getattr(message_bus, "decide_permission", None)
        if decide is None:
            return "Permission audit is unavailable"
        event_id = await decide(
            request_id=request_id,
            decision="approved",
            reason=reason or "Approved by lead agent",
            decided_by="lead",
        )
        return (
            f"Permission request '{request_id}' approved"
            if event_id
            else f"Error: Permission request '{request_id}' not found"
        )

    @tool
    async def reject_permission(request_id: str, reason: str) -> str:
        """Reject a permission request and publish its directed MQ response."""
        decide = getattr(message_bus, "decide_permission", None)
        if decide is None:
            return "Permission audit is unavailable"
        event_id = await decide(
            request_id=request_id,
            decision="rejected",
            reason=reason,
            decided_by="lead",
        )
        return (
            f"Permission request '{request_id}' rejected: {reason}"
            if event_id
            else f"Error: Permission request '{request_id}' not found"
        )

    return [
        spawn_sub_agent,
        assign_task,
        list_sub_agents,
        shutdown_agent,
        send_message,
        check_inbox,
        list_pending_permissions,
        approve_permission,
        reject_permission,
        publish_dag,
        get_task_board_status,
    ]
