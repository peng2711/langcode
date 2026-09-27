"""模型调用的错误恢复。

临时故障（429 / 5xx / 超时）的指数退避重试和备用模型切换交给 LangChain 官方的
ModelRetryMiddleware / ModelFallbackMiddleware（见 lib/agent_middleware.py）。
本中间件只处理官方没有覆盖的两类问题：
1. 输出截断（finish_reason == "length"）→ 续写并拼接
2. 上下文超限 → 兜底压缩后重试一次

两者都通过 handler 重新发起调用，保留工具绑定、system prompt 以及内层的重试。
"""

import logging
from typing import Any, Optional

from langchain.agents.middleware import AgentMiddleware, ModelResponse
from langchain_core.messages import AIMessage, HumanMessage

logger = logging.getLogger(__name__)

CONTEXT_EXCEEDED_MARKERS = (
    "context_length_exceeded",
    "prompt_too_long",
    "maximum context length",
    "too many tokens",
)

CONTINUE_PROMPT = "请继续完成上面的回答，直接从断点处继续，不要重复已输出的内容。"


class ContextLengthExceededError(Exception):
    """上下文超限且兜底压缩后仍失败"""


def is_context_exceeded(error: Exception) -> bool:
    error_str = str(error).lower()
    return any(marker in error_str for marker in CONTEXT_EXCEEDED_MARKERS)


def _last_ai_message(response: Any) -> Optional[AIMessage]:
    messages = response.result if isinstance(response, ModelResponse) else [response]
    last = messages[-1] if messages else None
    return last if isinstance(last, AIMessage) else None


class ErrorRecoveryMiddleware(AgentMiddleware):
    def __init__(
        self,
        context_compressor: Optional[Any] = None,  # ContextCompressionMiddleware 实例
        max_continuation_attempts: int = 2,
    ):
        self.context_compressor = context_compressor
        self.max_continuation_attempts = max_continuation_attempts

    async def awrap_model_call(self, request, handler):
        try:
            response = await handler(request)
        except Exception as e:
            if not is_context_exceeded(e):
                raise
            response = await self._retry_with_compaction(request, handler, e)

        last = _last_ai_message(response)
        if last is not None and self._is_truncation(last):
            return await self._continue_truncated(request, handler, last)
        return response

    @staticmethod
    def _is_truncation(message: AIMessage) -> bool:
        return message.response_metadata.get("finish_reason") == "length"

    async def _retry_with_compaction(self, request, handler, error: Exception):
        if self.context_compressor is None:
            raise ContextLengthExceededError("上下文超限且未配置压缩器") from error
        compacted = await self.context_compressor.reactive_compact(request.messages)
        logger.info(f"上下文超限，兜底压缩后重试：{len(request.messages)} → {len(compacted)} 条消息")
        try:
            return await handler(request.override(messages=compacted))
        except Exception as retry_error:
            raise ContextLengthExceededError(f"上下文超限处理失败: {retry_error}") from retry_error

    async def _continue_truncated(self, request, handler, partial: AIMessage) -> ModelResponse:
        """续写被截断的回复；续写次数达到上限后返回已有内容并标注。"""
        if not isinstance(partial.content, str):
            return ModelResponse(result=[partial])

        content = partial.content
        last = partial
        for attempt in range(1, self.max_continuation_attempts + 1):
            logger.info(f"检测到输出截断，发起第 {attempt} 次续写")
            continuation = request.override(
                messages=[
                    *request.messages,
                    AIMessage(content=content),
                    HumanMessage(content=CONTINUE_PROMPT),
                ]
            )
            try:
                last = _last_ai_message(await handler(continuation))
            except Exception as e:
                logger.error(f"续写失败：{e}")
                last = None
            if last is None or not isinstance(last.content, str):
                content += "\n\n[⚠️ 续写失败，回复可能不完整]"
                break
            content += last.content
            if not self._is_truncation(last):
                break
        else:
            content += "\n\n[⚠️ 回复被截断，已达到最大续写尝试次数]"

        merged = AIMessage(
            content=content,
            tool_calls=getattr(last, "tool_calls", []) if last is not None else [],
            response_metadata={
                **(last.response_metadata if last is not None else {}),
                "truncation_handled": True,
            },
        )
        return ModelResponse(result=[merged])
