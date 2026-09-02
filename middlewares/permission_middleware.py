"""Permission middleware backed by the MessageBus and PostgreSQL audit trail."""

from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path
from typing import Any
from uuid import uuid4

from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import ToolMessage


DANGEROUS_PATTERNS = [
    {"tool": "bash", "pattern": "rm -rf"},
    {"tool": "bash", "pattern": "sudo"},
    {"tool": "bash", "pattern": "chmod 777"},
    {"tool": "bash", "pattern": "curl.*\\|.*sh"},
    {"tool": "bash", "pattern": "wget.*\\|.*sh"},
    {"tool": "write_file", "pattern": "/etc/"},
    {"tool": "write_file", "pattern": "/usr/"},
    {"tool": "edit_file", "pattern": "/etc/"},
    {"tool": "edit_file", "pattern": "/usr/"},
]


class PermissionMiddleware(AgentMiddleware):
    """Apply hard denials and obtain audited asynchronous approvals."""

    def __init__(
        self,
        work_dir: Path = Path(os.getcwd()),
        message_bus: Any | None = None,
        agent_name: str = "default",
        consumer_name: str | None = None,
        thread_id: str | None = None,
        task_id: str | None = None,
        execution_id: str | None = None,
        attempt: int | None = None,
    ):
        self.WORK_DIR = work_dir
        self.DENY_LIST = [
            "rm -rf /",
            "sudo",
            "shutdown",
            "reboot",
            "mkfs",
            "dd if=",
            "> /dev/sda",
        ]
        self.PERMISSION_RULES = [
            {
                "tools": ["write_file", "edit_file"],
                "check": lambda args: not (
                    self.WORK_DIR
                    / args.get("file_path", args.get("path", ""))
                )
                .resolve()
                .is_relative_to(self.WORK_DIR),
                "message": "Writing outside workspace",
            },
            {
                "tools": ["bash"],
                "check": lambda args: any(
                    keyword in args.get("command", "")
                    for keyword in ["rm ", "> /etc/", "chmod 777"]
                ),
                "message": "Potentially dangerous command",
            },
        ]
        self.message_bus = message_bus
        self.agent_name = agent_name
        self.consumer_name = consumer_name or agent_name
        self.thread_id = thread_id
        self.task_id = task_id
        self.execution_id = execution_id
        self.attempt = attempt
        self._responses: dict[str, dict[str, Any]] = {}

    def check_deny_list(self, command: str) -> str | None:
        for pattern in self.DENY_LIST:
            if pattern in command:
                return f"Blocked: '{pattern}' is on the deny list"
        return None

    def check_rules(self, tool_name: str, args: dict[str, Any]) -> str | None:
        for rule in self.PERMISSION_RULES:
            if tool_name in rule["tools"] and rule["check"](args):
                return rule["message"]
        return None

    def ask_user(self, tool_name: str, args: dict[str, Any], reason: str) -> bool:
        print(f"\nPermission required: {reason}")
        print(f"Tool: {tool_name}({args})")
        choice = input("Allow? [y/N] ").strip().lower()
        return choice in ("y", "yes")

    async def _wait_for_permission_decision(
        self,
        request_id: str,
        timeout: int = 300,
    ) -> dict[str, Any]:
        cached = self._responses.pop(request_id, None)
        if cached is not None:
            return cached

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            remaining = max(0.1, min(5, deadline - time.monotonic()))
            messages = await self.message_bus.receive(
                self.consumer_name,
                timeout=remaining,
            )
            for message in messages:
                try:
                    if message.event_type != "permission_response":
                        await self.message_bus.ack(message.event_id)
                        continue
                    content = message.payload
                    response_request_id = content.get("request_id")
                    if response_request_id:
                        self._responses[response_request_id] = content
                    await self.message_bus.ack(message.event_id)
                except Exception:
                    await self.message_bus.retry(
                        message.event_id,
                        "permission response handling failed",
                    )
                    raise
            cached = self._responses.pop(request_id, None)
            if cached is not None:
                return cached

        return {"decision": "rejected", "reason": "Timeout waiting for approval"}

    async def _request_permission(
        self,
        *,
        tool_name: str,
        command: str,
        reason: str,
    ) -> dict[str, Any]:
        if self.message_bus is None:
            return {
                "decision": (
                    "approved"
                    if self.ask_user(tool_name, {"command": command}, reason)
                    else "rejected"
                ),
                "reason": reason,
            }

        request_id = f"perm_{uuid4().hex}"
        create_request = getattr(self.message_bus, "create_permission_request", None)
        if create_request is not None:
            await create_request(
                request_id=request_id,
                agent_name=self.agent_name,
                tool_name=tool_name,
                command=command,
                thread_id=self.thread_id,
                task_id=self.task_id,
                execution_id=self.execution_id,
                attempt=self.attempt,
            )
        else:
            await self.message_bus.send(
                sender=self.agent_name,
                target="lead",
                event_type="permission.requested",
                payload={
                    "request_id": request_id,
                    "tool": tool_name,
                    "command": command,
                    "reason": reason,
                },
                thread_id=self.thread_id,
                task_id=self.task_id,
                execution_id=self.execution_id,
                attempt=self.attempt,
                request_id=request_id,
            )
        return await self._wait_for_permission_decision(request_id)

    async def awrap_tool_call(self, request: Any, handler: Any) -> ToolMessage:
        tool_name = request.tool_call["name"]
        tool_args = request.tool_call.get("args", {})
        tool_id = request.tool_call["id"]

        if tool_name == "bash":
            command = tool_args.get("command", "")
            reason = self.check_deny_list(command)
            if reason:
                return ToolMessage(
                    content=f"Permission denied: {reason}",
                    tool_call_id=tool_id,
                )

        matched_pattern = next(
            (
                rule
                for rule in DANGEROUS_PATTERNS
                if tool_name == rule["tool"]
                and rule["pattern"]
                in (
                    tool_args.get("command", "")
                    or tool_args.get("file_path", "")
                    or tool_args.get("path", "")
                )
            ),
            None,
        )
        reason = (
            f"Operation matches dangerous pattern: {matched_pattern['pattern']}"
            if matched_pattern
            else self.check_rules(tool_name, tool_args)
        )
        if reason:
            command = (
                tool_args.get("command", "")
                or tool_args.get("file_path", "")
                or tool_args.get("path", "")
            )
            decision = await self._request_permission(
                tool_name=tool_name,
                command=command,
                reason=reason,
            )
            if decision.get("decision") != "approved":
                return ToolMessage(
                    content=(
                        "Permission denied: "
                        f"{decision.get('reason', 'No reason provided')}"
                    ),
                    tool_call_id=tool_id,
                )

        return await handler(request)
