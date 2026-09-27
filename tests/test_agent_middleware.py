"""Middleware behaviour through a real create_agent graph with scripted fake models."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any

import httpx
import openai
from langchain.agents import create_agent
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.tools import tool
from langgraph.store.memory import InMemoryStore
from pydantic import Field

from lib.agent_middleware import build_context_and_recovery_middleware
from middlewares.memory_management_middleware import MemoryManagementMiddleware
from middlewares.skill_loading_middleware import SkillLoadingMiddleware


class ScriptedModel(BaseChatModel):
    """Returns scripted replies in order; an Exception entry is raised instead."""

    responses: list[Any] = Field(default_factory=list)
    calls: list[list[Any]] = Field(default_factory=list)

    def _generate(self, messages, stop=None, run_manager=None, **kwargs) -> ChatResult:
        self.calls.append(list(messages))
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return ChatResult(generations=[ChatGeneration(message=item)])

    def bind_tools(self, tools, **kwargs):
        return self

    @property
    def _llm_type(self) -> str:
        return "scripted"


@tool
def echo(text: str) -> str:
    """Return the given text."""
    return text


def _api_error(cls, status: int, message: str):
    response = httpx.Response(status, request=httpx.Request("POST", "https://example.invalid"))
    return cls(message, response=response, body=None)


def _tool_call(call_id: str, text: str = "x") -> AIMessage:
    return AIMessage(content="", tool_calls=[{"name": "echo", "args": {"text": text}, "id": call_id}])


def _run(agent, messages):
    return asyncio.run(agent.ainvoke({"messages": messages}))


def _agent(model, light, *, extra=(), **kwargs):
    kwargs.setdefault("max_retries", 2)
    kwargs.setdefault("retry_initial_delay", 0)
    return create_agent(
        model=model,
        tools=[echo],
        system_prompt="SYS",
        middleware=[*extra, *build_context_and_recovery_middleware(light, **kwargs)],
    )


def _system_text(call: list[Any]) -> str:
    assert isinstance(call[0], SystemMessage)
    return call[0].text


def _orphan_tool_calls(messages) -> set[str]:
    """tool_call 没有对应结果，或结果没有对应 tool_call，两种都算配对被拆散。"""
    called = {tc["id"] for m in messages if isinstance(m, AIMessage) for tc in m.tool_calls}
    answered = {m.tool_call_id for m in messages if isinstance(m, ToolMessage)}
    return called ^ answered


def test_skills_are_injected_and_skill_file_edits_are_detected(tmp_path):
    skill_file = tmp_path / "skills" / "demo" / "SKILL.md"
    skill_file.parent.mkdir(parents=True)
    skill_file.write_text("---\nname: demo\ndescription: first version\n---\nbody")
    model = ScriptedModel(responses=[AIMessage("ok"), AIMessage("ok")])
    agent = _agent(model, ScriptedModel(), extra=[SkillLoadingMiddleware(tmp_path)])

    _run(agent, [HumanMessage("hi")])
    assert "demo: first version" in _system_text(model.calls[0])

    skill_file.write_text("---\nname: demo\ndescription: second version\n---\nbody")
    stat = skill_file.stat()
    os.utime(skill_file, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))
    _run(agent, [HumanMessage("hi")])
    assert "demo: second version" in _system_text(model.calls[1])


def test_memories_are_recalled_once_per_turn_and_injected_into_every_model_call():
    store = InMemoryStore()
    store.put(("user_id", "memories"), "k1", {
        "type": "semantic", "content": "用户偏好 Go", "description": "语言偏好",
    })
    memory_llm = ScriptedModel(responses=[
        AIMessage("k1"),
        AIMessage('{"semantic": [], "procedural": [], "episodic": []}'),
    ])
    model = ScriptedModel(responses=[_tool_call("c1"), AIMessage("done")])
    agent = _agent(model, ScriptedModel(), extra=[MemoryManagementMiddleware(llm=memory_llm, store=store)])

    _run(agent, [HumanMessage("写个服务")])

    selections = [c for c in memory_llm.calls if "记忆检索助手" in c[0].text]
    assert len(selections) == 1
    assert len(model.calls) == 2
    assert all("用户偏好 Go" in _system_text(call) for call in model.calls)


def test_transient_error_is_retried_on_the_primary_model():
    model = ScriptedModel(responses=[_api_error(openai.RateLimitError, 429, "rate limit"), AIMessage("ok")])
    light = ScriptedModel()

    result = _run(_agent(model, light), [HumanMessage("hi")])

    assert result["messages"][-1].content == "ok"
    assert len(model.calls) == 2 and not light.calls


def test_falls_back_to_light_model_after_retries_are_exhausted():
    model = ScriptedModel(responses=[_api_error(openai.InternalServerError, 529, "overloaded")] * 3)
    light = ScriptedModel(responses=[AIMessage("from fallback")])

    result = _run(_agent(model, light, max_retries=2), [HumanMessage("hi")])

    assert result["messages"][-1].content == "from fallback"
    assert len(model.calls) == 3


def test_context_exceeded_compacts_keeps_tool_pairs_and_system_prompt():
    model = ScriptedModel(responses=[
        _api_error(openai.BadRequestError, 400, "maximum context length exceeded"),
        AIMessage("ok"),
    ])
    light = ScriptedModel(responses=[AIMessage("summary of earlier work")])
    history = [
        HumanMessage("start"),
        AIMessage(content="", tool_calls=[
            {"name": "echo", "args": {"text": "a"}, "id": "c1"},
            {"name": "echo", "args": {"text": "b"}, "id": "c2"},
        ]),
        ToolMessage("a", tool_call_id="c1"),
        ToolMessage("b", tool_call_id="c2"),
        AIMessage("mid"),
        HumanMessage("q1"),
        AIMessage("a1"),
        HumanMessage("q2"),
    ]

    result = _run(_agent(model, light), history)

    assert result["messages"][-1].content == "ok"
    retried = model.calls[1]
    assert _system_text(retried) == "SYS"
    assert "summary of earlier work" in retried[1].content
    assert not _orphan_tool_calls(retried)
    assert not any(m.content == "start" for m in retried[1:])


def test_truncated_reply_is_continued_through_the_agent_model():
    model = ScriptedModel(responses=[
        AIMessage("part1 ", response_metadata={"finish_reason": "length"}),
        AIMessage("part2", response_metadata={"finish_reason": "stop"}),
    ])

    result = _run(_agent(model, ScriptedModel()), [HumanMessage("hi")])

    assert result["messages"][-1].content == "part1 part2"
    assert _system_text(model.calls[1]) == "SYS"
    assert "继续" in model.calls[1][-1].content


def test_oversized_tool_result_is_offloaded_before_entering_state(tmp_path):
    model = ScriptedModel(responses=[_tool_call("c1", "y" * 500), AIMessage("done")])

    result = _run(
        _agent(model, ScriptedModel(), tool_result_size_threshold=100, result_dir=tmp_path),
        [HumanMessage("hi")],
    )

    tool_msg = next(m for m in result["messages"] if isinstance(m, ToolMessage))
    assert tool_msg.content.startswith("[Tool result too large: 500 chars]")
    saved = list(Path(tmp_path).glob("tool_result_*.txt"))
    assert len(saved) == 1 and saved[0].read_text() == "y" * 500


def test_summarization_shrinks_state_without_orphaning_tool_calls():
    steps = 8
    model = ScriptedModel(responses=[_tool_call(f"c{i}", "z" * 200) for i in range(steps)] + [AIMessage("done")])
    light = ScriptedModel(responses=[AIMessage("summary")] * 20)

    result = _run(
        _agent(model, light, summarize_trigger_tokens=300, summarize_keep_messages=4),
        [HumanMessage("hi")],
    )

    messages = result["messages"]
    produced = 1 + 2 * steps + 1
    assert len(messages) < produced
    assert light.calls, "summarization model was never called"
    assert not _orphan_tool_calls(messages)
    assert messages[-1].content == "done"
