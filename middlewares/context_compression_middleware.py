"""上下文压缩中的自研部分。

完整管线（由 lib/agent_middleware.py 组装，按成本从低到高）：
1. ToolResultBudget（本文件）：工具返回超大结果时直接落盘，只把预览写入消息，
   大结果从一开始就不进入 state 和 checkpoint。
2. ContextEditingMiddleware（LangChain 官方）：清理较早的工具结果，只改发给模型的请求。
3. SummarizationMiddleware（LangChain 官方）：超过阈值时用 LLM 摘要替换 state 中的旧消息，
   并保证 AI tool_call 与 ToolMessage 不被拆开。
4. reactive_compact（本文件）：模型调用仍报上下文超限时，由 ErrorRecoveryMiddleware
   调用做兜底压缩后重试，只影响本次请求。
"""

import hashlib
import logging
import time
from pathlib import Path
from typing import List

import aiofiles
from langchain.agents.middleware import AgentMiddleware
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import BaseMessage, HumanMessage, ToolMessage

logger = logging.getLogger(__name__)

SUMMARY_PROMPT = (
    "Summarize this coding-agent conversation so work can continue.\n"
    "Preserve: 1. current goal, 2. key findings/decisions, 3. files read/changed, "
    "4. remaining work, 5. user constraints.\nBe compact but concrete, no more than 1000 tokens.\n\n"
)


class ContextCompressionMiddleware(AgentMiddleware):
    """超大工具结果落盘，并提供超限兜底压缩。"""

    def __init__(
        self,
        llm: BaseChatModel,                              # 用于兜底摘要的轻量模型
        tool_result_size_threshold: int = 200 * 1024,    # 200KB
        tool_preview_chars: int = 2000,
        result_dir: Path = Path("/tmp/langchain_mem"),
        reactive_keep_messages: int = 5,
    ):
        self.llm = llm
        self.tool_result_size_threshold = tool_result_size_threshold
        self.tool_preview_chars = tool_preview_chars
        self.result_dir = result_dir
        self.reactive_keep_messages = reactive_keep_messages

    async def awrap_tool_call(self, request, handler):
        """ToolResultBudget：工具结果超过阈值时落盘，消息中只保留文件路径和预览。"""
        result = await handler(request)
        if (
            isinstance(result, ToolMessage)
            and isinstance(result.content, str)
            and len(result.content) > self.tool_result_size_threshold
        ):
            path = await self._save_to_disk(result.content)
            preview = result.content[:self.tool_preview_chars]
            result.content = (
                f"[Tool result too large: {len(result.content)} chars] Full output saved to: {path}\n"
                f"Use read_file on that path if you need more.\nPreview:\n{preview}"
            )
        return result

    async def reactive_compact(self, messages: List[BaseMessage]) -> List[BaseMessage]:
        """兜底压缩：LLM 摘要 + 最近几条消息。

        保留区间的起点如果落在 ToolMessage 上，就一直向前移动到发起这些调用的 AIMessage，
        保证并行工具调用的所有结果和对应的 tool_call 一起保留。
        """
        if not messages:
            return messages
        summary = await self._summarize_messages(messages)
        tail_start = max(0, len(messages) - self.reactive_keep_messages)
        while tail_start > 0 and isinstance(messages[tail_start], ToolMessage):
            tail_start -= 1
        return [HumanMessage(content=f"[Reactive Compact] Summary: {summary}")] + messages[tail_start:]

    async def _summarize_messages(self, messages: List[BaseMessage]) -> str:
        conv_text = "\n".join(f"{m.__class__.__name__}: {str(m.content)[:500]}" for m in messages)
        response = await self.llm.with_config(
            tags=["internal_memory_call"],
            metadata={"internal": True},
        ).ainvoke(SUMMARY_PROMPT + conv_text)
        return response.content if hasattr(response, "content") else str(response)

    async def _save_to_disk(self, content: str) -> Path:
        self.result_dir.mkdir(parents=True, exist_ok=True)
        digest = hashlib.md5(content.encode()).hexdigest()[:8]
        path = self.result_dir / f"tool_result_{digest}_{int(time.time())}.txt"
        async with aiofiles.open(path, "w") as f:
            await f.write(content)
        return path
