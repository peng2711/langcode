"""Generic Agent Runtime with load-on-claim Agent Cards.

The module retains its historical filename for import compatibility, but no
longer models an independently-addressable sub agent. Every process is a
generic runtime that claims work from the shared RocketMQ consumer group.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from langchain.agents import create_agent
from langchain_core.messages import AIMessage
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from lib.dag_scheduler import ClaimedExecution, DAGScheduler
from lib.message_hub import (
    AGENT_COMMAND_TOPIC,
    AGENT_MESSAGE_TOPIC,
    MessageBus,
    MessageEnvelope,
    RocketMQMessageBus,
)
from middlewares.context_compression_middleware import ContextCompressionMiddleware
from middlewares.error_recovery_middleware import ErrorRecoveryMiddleware
from middlewares.permission_middleware import PermissionMiddleware
from middlewares.skill_loading_middleware import SkillLoadingMiddleware

logger = logging.getLogger(__name__)


@dataclass
class RuntimeMailbox:
    """In-memory view of normal MQ messages delivered to a Runtime."""

    messages: deque[MessageEnvelope] = field(default_factory=deque)

    def add(self, message: MessageEnvelope) -> None:
        self.messages.append(message)

    def drain(self) -> list[MessageEnvelope]:
        messages = list(self.messages)
        self.messages.clear()
        return messages


@dataclass
class RuntimeState:
    """Mutable process-local state for one generic Runtime."""

    active_claim: ClaimedExecution | None = None
    active_task: asyncio.Task[Any] | None = None
    mailbox: RuntimeMailbox = field(default_factory=RuntimeMailbox)
    wake_requested: asyncio.Event = field(default_factory=asyncio.Event)
    stop_requested: bool = False


async def create_sub_agent(
    *,
    runtime_id: str,
    task: ClaimedExecution,
    llm: ChatOpenAI,
    checkpointer: AsyncPostgresSaver,
    message_bus: MessageBus,
    work_dir: Path,
    light_llm: ChatOpenAI | None = None,
    permission_consumer: str | None = None,
    mailbox: RuntimeMailbox | None = None,
) -> Any:
    """Build an agent using only the Card frozen by the claim transaction."""
    tool_factories = {
        "bash": _create_bash_tool,
        "read_file": _create_read_file_tool,
        "write_file": _create_write_file_tool,
        "edit_file": _create_edit_file_tool,
        "glob": _create_glob_tool,
    }
    communication_tools = {"send_message_to_lead", "check_runtime_inbox"}
    unknown_tools = sorted(
        set(task.card_tool_allowlist) - set(tool_factories) - communication_tools
    )
    if unknown_tools:
        raise ValueError(
            f"Card {task.agent_card_id}@{task.agent_card_version} references "
            f"unsupported tools: {', '.join(unknown_tools)}"
        )
    tools = [
        await tool_factories[name]()
        for name in task.card_tool_allowlist
        if name in tool_factories
    ]
    runtime_mailbox = mailbox or RuntimeMailbox()

    @tool
    async def send_message_to_lead(
        content: str,
        msg_type: str = "message",
    ) -> str:
        """Send a normal execution-scoped message to the lead through MQ."""
        await message_bus.send(
            sender=runtime_id,
            target="lead",
            event_type=msg_type,
            payload={"text": content},
            thread_id=task.thread_id,
            task_id=task.task_id,
            execution_id=task.execution_id,
            attempt=task.attempt,
        )
        return "Message queued for lead"

    @tool
    def check_runtime_inbox() -> str:
        """Read and clear normal MQ messages sent to this Runtime."""
        messages = runtime_mailbox.drain()
        if not messages:
            return "Inbox is empty"
        return "\n".join(
            f"From: {message.sender} | Type: {message.event_type} | "
            f"Content: {message.payload}"
            for message in messages
        )

    if "send_message_to_lead" in task.card_tool_allowlist:
        tools.append(send_message_to_lead)
    if "check_runtime_inbox" in task.card_tool_allowlist:
        tools.append(check_runtime_inbox)

    permission_middleware = PermissionMiddleware(
        work_dir=work_dir,
        message_bus=message_bus,
        agent_name=runtime_id,
        consumer_name=permission_consumer or runtime_id,
        thread_id=task.thread_id,
        task_id=task.task_id,
        execution_id=task.execution_id,
        attempt=task.attempt,
    )
    middleware: list[Any] = [permission_middleware]
    middleware.append(
        SkillLoadingMiddleware(
            work_dir,
            allowed_skill_names=set(task.card_skill_allowlist),
        )
    )
    if light_llm:
        context_compression = ContextCompressionMiddleware(llm=light_llm)
        middleware.extend(
            [
                context_compression,
                ErrorRecoveryMiddleware(
                    primary_llm=llm,
                    fallback_llm=light_llm,
                    context_compressor=context_compression,
                    max_retries=5,
                    max_continuation_attempts=2,
                    max_tokens_for_continuation=64000,
                    consecutive_529_threshold=3,
                ),
            ]
        )

    system_prompt = (
        f"{task.card_system_prompt}\n\n"
        f"Runtime: {runtime_id}\n"
        f"Working directory: {work_dir}\n"
        f"Task: {task.description or task.subject}\n"
        "Complete only this task. Do not assume identities or permissions "
        "outside the frozen Agent Card."
    )
    return create_agent(
        model=llm,
        tools=tools,
        system_prompt=system_prompt,
        middleware=middleware,
        checkpointer=checkpointer,
    )


async def run_sub_agent(
    *,
    runtime_id: str,
    llm: ChatOpenAI,
    checkpointer: AsyncPostgresSaver,
    message_bus: RocketMQMessageBus,
    work_dir: Path,
    light_llm: ChatOpenAI | None = None,
    max_rounds: int = 30,
    runtime_version: str = "v1",
) -> None:
    """Run a generic Agent Runtime until the caller cancels the task."""
    scheduler = DAGScheduler(pool=message_bus.pool)
    await scheduler.register_runtime(runtime_id, runtime_version=runtime_version)

    work_consumer = f"runtime-work-{runtime_id}"
    permission_consumer = f"runtime-permission-{runtime_id}"
    control_consumer = f"runtime-control-{runtime_id}"
    inbox_consumer = f"runtime-inbox-{runtime_id}"
    await message_bus.subscribe(
        work_consumer,
        topic=AGENT_COMMAND_TOPIC,
        tag_expression="task_available",
        group_id="GID-agent-runtime-pool",
    )
    await message_bus.subscribe(
        permission_consumer,
        topic=AGENT_COMMAND_TOPIC,
        tag_expression="permission_response",
        target=runtime_id,
        allow_broadcast=False,
        group_id=f"GID-agent-runtime-permission-{runtime_id}",
    )
    await message_bus.subscribe(
        control_consumer,
        topic=AGENT_COMMAND_TOPIC,
        tag_expression="execution_cancel || runtime_wakeup",
        target=runtime_id,
        allow_broadcast=False,
        group_id=f"GID-agent-runtime-control-{runtime_id}",
    )
    await message_bus.subscribe(
        inbox_consumer,
        topic=AGENT_MESSAGE_TOPIC,
        tag_expression="*",
        target=runtime_id,
        allow_broadcast=False,
        group_id=f"GID-agent-runtime-inbox-{runtime_id}",
    )

    state = RuntimeState()
    state.wake_requested.set()
    heartbeat_task = asyncio.create_task(
        _runtime_heartbeat_loop(scheduler, runtime_id)
    )
    control_task = asyncio.create_task(
        _runtime_control_loop(
            scheduler=scheduler,
            message_bus=message_bus,
            consumer=control_consumer,
            runtime_id=runtime_id,
            state=state,
        )
    )
    inbox_task = asyncio.create_task(
        _runtime_inbox_loop(
            message_bus=message_bus,
            consumer=inbox_consumer,
            runtime_id=runtime_id,
            mailbox=state.mailbox,
        )
    )
    last_compensation_scan = 0.0

    async def execute_claim(claim: ClaimedExecution) -> None:
        state.active_claim = claim
        execution_task = asyncio.create_task(
            _run_claimed_execution(
                scheduler=scheduler,
                runtime_id=runtime_id,
                claim=claim,
                llm=llm,
                checkpointer=checkpointer,
                message_bus=message_bus,
                work_dir=work_dir,
                light_llm=light_llm,
                max_rounds=max_rounds,
                permission_consumer=permission_consumer,
                mailbox=state.mailbox,
            )
        )
        state.active_task = execution_task
        try:
            await execution_task
        except asyncio.CancelledError:
            logger.info(
                "Execution cancelled by Runtime control message",
                extra={
                    "runtime_id": runtime_id,
                    "task_id": claim.task_id,
                    "execution_id": claim.execution_id,
                    "attempt": claim.attempt,
                },
            )
        finally:
            state.active_task = None
            state.active_claim = None
            # A cancellation can arrive before the execution coroutine reaches
            # its own finally block. The scheduler operation is idempotent.
            await scheduler.finish_card_unload(
                task_id=claim.task_id,
                execution_id=claim.execution_id,
                attempt=claim.attempt,
                runtime_id=runtime_id,
            )

    try:
        while not state.stop_requested:
            messages = await message_bus.receive(work_consumer, timeout=5)
            if (
                state.wake_requested.is_set()
                or (
                    not messages
                    and asyncio.get_running_loop().time() - last_compensation_scan >= 15
                )
            ):
                state.wake_requested.clear()
                last_compensation_scan = asyncio.get_running_loop().time()
                for shard in await scheduler.get_ready_shards(limit=64):
                    claim = await scheduler.claim_next_available_task(
                        thread_id=shard["thread_id"],
                        runtime_id=runtime_id,
                        task_type=shard["task_type"],
                        work_shard=shard["work_shard"],
                    )
                    if claim is None:
                        continue
                    await execute_claim(claim)
                    break
            for message in messages:
                try:
                    if message.event_type != "task_available":
                        await message_bus.ack(message.event_id)
                        continue
                    task_type = str(message.payload["task_type"])
                    work_shard = int(message.payload["work_shard"])
                    if message.thread_id is None:
                        await message_bus.ack(message.event_id)
                        continue
                    claim = await scheduler.claim_next_available_task(
                        thread_id=message.thread_id,
                        runtime_id=runtime_id,
                        task_type=task_type,
                        work_shard=work_shard,
                    )
                    # A task_available message is only a wakeup hint. Once the
                    # transactional claim has resolved (including "no task"),
                    # its work is complete and it must be acknowledged.
                    await message_bus.ack(message.event_id)
                    if claim is None:
                        continue
                    await execute_claim(claim)
                except Exception as exc:
                    logger.exception(
                        "Failed to process task availability event",
                        extra={"event_id": message.event_id, "runtime_id": runtime_id},
                    )
                    try:
                        await message_bus.retry(message.event_id, str(exc))
                    except KeyError:
                        # Claim success is acknowledged before execution to avoid
                        # coupling a long LLM call to a broker callback.
                        pass
    finally:
        for task in (heartbeat_task, control_task, inbox_task):
            task.cancel()
        for task in (heartbeat_task, control_task, inbox_task):
            try:
                await task
            except asyncio.CancelledError:
                pass
        await scheduler.stop_runtime(runtime_id)


async def _run_claimed_execution(
    *,
    scheduler: DAGScheduler,
    runtime_id: str,
    claim: ClaimedExecution,
    llm: ChatOpenAI,
    checkpointer: AsyncPostgresSaver,
    message_bus: MessageBus,
    work_dir: Path,
    light_llm: ChatOpenAI | None,
    max_rounds: int,
    permission_consumer: str,
    mailbox: RuntimeMailbox,
) -> None:
    """Load the frozen Card, execute, and always unload it before reuse."""
    load_started = await scheduler.mark_card_load_started(
        task_id=claim.task_id,
        execution_id=claim.execution_id,
        attempt=claim.attempt,
        runtime_id=runtime_id,
    )
    if not load_started:
        return

    agent: Any | None = None
    execution_heartbeat_task = asyncio.create_task(
        _execution_heartbeat_loop(
            scheduler=scheduler,
            claim=claim,
            runtime_id=runtime_id,
            execution_task=asyncio.current_task(),
        )
    )
    try:
        try:
            agent = await create_sub_agent(
                runtime_id=runtime_id,
                task=claim,
                llm=llm,
                checkpointer=checkpointer,
                message_bus=message_bus,
                work_dir=work_dir,
                light_llm=light_llm,
                permission_consumer=permission_consumer,
                mailbox=mailbox,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await scheduler.mark_card_load_failed(
                task_id=claim.task_id,
                execution_id=claim.execution_id,
                attempt=claim.attempt,
                runtime_id=runtime_id,
                error=str(exc),
            )
            return

        started = await scheduler.mark_execution_started(
            task_id=claim.task_id,
            execution_id=claim.execution_id,
            attempt=claim.attempt,
            runtime_id=runtime_id,
        )
        if not started:
            return

        messages = [{"role": "user", "content": claim.description or claim.subject}]
        config = {
            "configurable": {
                "thread_id": (
                    f"{claim.thread_id}_{claim.execution_id}_{claim.attempt}"
                )
            }
        }
        result: str | None = None
        for _ in range(max_rounds):
            response = await agent.ainvoke({"messages": messages}, config=config)
            last_message = response["messages"][-1]
            if isinstance(last_message, AIMessage) and not last_message.tool_calls:
                result = str(last_message.content)
                break
            messages = response["messages"]

        if result is None:
            await scheduler.fail_execution(
                task_id=claim.task_id,
                execution_id=claim.execution_id,
                attempt=claim.attempt,
                runtime_id=runtime_id,
                error="Task execution reached max rounds without a final response",
            )
            return

        result_path = (
            f"/tmp/task_results/{claim.thread_id or 'default'}/{claim.task_id}"
            f"-{claim.execution_id}-{claim.attempt}.txt"
        )
        os.makedirs(os.path.dirname(result_path), exist_ok=True)
        with open(result_path, "w", encoding="utf-8") as file:
            file.write(result)
        await scheduler.complete_execution(
            task_id=claim.task_id,
            execution_id=claim.execution_id,
            attempt=claim.attempt,
            runtime_id=runtime_id,
            summary=result,
            result_path=result_path,
        )
    except Exception as exc:
        logger.exception(
            "Execution failed",
            extra={
                "runtime_id": runtime_id,
                "task_id": claim.task_id,
                "execution_id": claim.execution_id,
                "attempt": claim.attempt,
            },
        )
        await scheduler.fail_execution(
            task_id=claim.task_id,
            execution_id=claim.execution_id,
            attempt=claim.attempt,
            runtime_id=runtime_id,
            error=str(exc),
        )
    finally:
        # Agent/Card objects are scoped to this attempt and deliberately not
        # retained by the Runtime after completion or failure.
        if execution_heartbeat_task is not None:
            execution_heartbeat_task.cancel()
            try:
                await execution_heartbeat_task
            except asyncio.CancelledError:
                pass
        if agent is not None:
            del agent
        await scheduler.finish_card_unload(
            task_id=claim.task_id,
            execution_id=claim.execution_id,
            attempt=claim.attempt,
            runtime_id=runtime_id,
        )


async def _runtime_heartbeat_loop(
    scheduler: DAGScheduler,
    runtime_id: str,
) -> None:
    while True:
        await asyncio.sleep(20)
        try:
            await scheduler.heartbeat_runtime(runtime_id)
            await scheduler.reclaim_leased_tasks()
        except Exception:
            logger.exception("Runtime heartbeat failed", extra={"runtime_id": runtime_id})


async def _execution_heartbeat_loop(
    *,
    scheduler: DAGScheduler,
    claim: ClaimedExecution,
    runtime_id: str,
    execution_task: asyncio.Task[Any] | None,
) -> None:
    """Renew the current task lease while the Runtime executes an attempt."""
    interval = max(1, min(20, scheduler.lease_duration // 2))
    while True:
        await asyncio.sleep(interval)
        try:
            renewed = await scheduler.renew_lease(
                task_id=claim.task_id,
                execution_id=claim.execution_id,
                attempt=claim.attempt,
                runtime_id=runtime_id,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "Execution lease renewal failed",
                extra={
                    "runtime_id": runtime_id,
                    "task_id": claim.task_id,
                    "execution_id": claim.execution_id,
                    "attempt": claim.attempt,
                },
            )
            continue
        if not renewed:
            logger.warning(
                "Execution lease could not be renewed; stopping local attempt",
                extra={
                    "runtime_id": runtime_id,
                    "task_id": claim.task_id,
                    "execution_id": claim.execution_id,
                    "attempt": claim.attempt,
                },
            )
            if execution_task is not None and not execution_task.done():
                execution_task.cancel()
            return


async def _runtime_control_loop(
    *,
    scheduler: DAGScheduler,
    message_bus: RocketMQMessageBus,
    consumer: str,
    runtime_id: str,
    state: RuntimeState,
) -> None:
    """Process directed wakeup and cancellation controls while work runs."""
    while True:
        try:
            messages = await message_bus.receive(consumer, timeout=5)
            for message in messages:
                try:
                    if message.event_type == "runtime_wakeup":
                        state.wake_requested.set()
                    elif message.event_type == "execution_cancel":
                        claim = state.active_claim
                        requested_execution_id = message.execution_id
                        requested_attempt = message.attempt
                        if (
                            claim is not None
                            and (
                                requested_execution_id is None
                                or (
                                    requested_execution_id == claim.execution_id
                                    and requested_attempt == claim.attempt
                                )
                            )
                        ):
                            cancelled = await scheduler.cancel_execution(
                                task_id=claim.task_id,
                                execution_id=claim.execution_id,
                                attempt=claim.attempt,
                                runtime_id=runtime_id,
                                reason=str(
                                    message.payload.get(
                                        "reason", "cancelled by lead"
                                    )
                                ),
                            )
                            if (
                                cancelled
                                and state.active_task is not None
                                and not state.active_task.done()
                            ):
                                state.active_task.cancel()
                        if bool(message.payload.get("stop_runtime")):
                            state.stop_requested = True
                    await message_bus.ack(message.event_id)
                except Exception as exc:
                    logger.exception(
                        "Failed to process Runtime control message",
                        extra={
                            "runtime_id": runtime_id,
                            "event_id": message.event_id,
                        },
                    )
                    await message_bus.retry(message.event_id, str(exc))
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "Runtime control consumer failed",
                extra={"runtime_id": runtime_id},
            )
            await asyncio.sleep(1)


async def _runtime_inbox_loop(
    *,
    message_bus: RocketMQMessageBus,
    consumer: str,
    runtime_id: str,
    mailbox: RuntimeMailbox,
) -> None:
    """Receive normal lead/Runtime messages without coupling them to claims."""
    while True:
        try:
            messages = await message_bus.receive(consumer, timeout=5)
            for message in messages:
                try:
                    mailbox.add(message)
                    await message_bus.ack(message.event_id)
                except Exception as exc:
                    logger.exception(
                        "Failed to process Runtime inbox message",
                        extra={
                            "runtime_id": runtime_id,
                            "event_id": message.event_id,
                        },
                    )
                    await message_bus.retry(message.event_id, str(exc))
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "Runtime inbox consumer failed",
                extra={"runtime_id": runtime_id},
            )
            await asyncio.sleep(1)


async def _create_bash_tool() -> Any:
    @tool
    def bash(command: str) -> str:
        """Execute a Bash command in the current workspace."""
        work_dir = Path(os.getcwd())
        try:
            result = __import__("subprocess").run(
                command,
                shell=True,
                cwd=work_dir,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=120,
            )
            output = (result.stdout + result.stderr).strip()
            return output[:50000] if output else "(no output)"
        except __import__("subprocess").TimeoutExpired:
            return "Error: Timeout (120s)"
        except (FileNotFoundError, OSError) as exc:
            return f"Error: {exc}"

    return bash


async def _create_read_file_tool() -> Any:
    @tool
    def read_file(file_path: str) -> str:
        """Read a UTF-8 local file."""
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                return file.read()
        except Exception as exc:
            return f"Read failed: {exc}"

    return read_file


async def _create_write_file_tool() -> Any:
    @tool
    def write_file(file_path: str, content: str) -> str:
        """Write a UTF-8 local file."""
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "w", encoding="utf-8") as file:
                file.write(content)
            return f"Wrote {file_path}"
        except Exception as exc:
            return f"Write failed: {exc}"

    return write_file


async def _create_edit_file_tool() -> Any:
    work_dir = Path(os.getcwd())

    @tool
    def edit_file(path: str, old_text: str, new_text: str) -> str:
        """Replace one exact text occurrence in a workspace file."""
        try:
            file_path = (work_dir / path).resolve()
            if not file_path.is_relative_to(work_dir):
                return f"Error: Path escapes workspace: {path}"
            text = file_path.read_text(encoding="utf-8")
            if old_text not in text:
                return f"Error: text not found in {path}"
            file_path.write_text(text.replace(old_text, new_text, 1), encoding="utf-8")
            return f"Edited {path}"
        except Exception as exc:
            return f"Error: {exc}"

    return edit_file


async def _create_glob_tool() -> Any:
    import glob as glob_module

    work_dir = Path(os.getcwd())

    @tool
    def glob(pattern: str) -> str:
        """List workspace files that match a glob pattern."""
        try:
            results = []
            for match in glob_module.glob(pattern, root_dir=work_dir):
                match_path = (work_dir / match).resolve()
                if match_path.is_relative_to(work_dir):
                    results.append(match)
            return "\n".join(results) if results else "(no matches)"
        except Exception as exc:
            return f"Error: {exc}"

    return glob
