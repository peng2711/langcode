"""Lead 与 Sub Agent 共用的上下文治理与错误恢复中间件栈。"""

from __future__ import annotations

from pathlib import Path

import openai
from langchain.agents.middleware import (
    AgentMiddleware,
    ClearToolUsesEdit,
    ContextEditingMiddleware,
    ModelFallbackMiddleware,
    ModelRetryMiddleware,
    SummarizationMiddleware,
)
from langchain_core.language_models import BaseChatModel

from middlewares.context_compression_middleware import ContextCompressionMiddleware
from middlewares.error_recovery_middleware import ErrorRecoveryMiddleware

# 只重试临时故障：429、5xx（含 529 过载）、超时和连接错误。
# 上下文超限等 4xx 不重试，交给 ErrorRecoveryMiddleware 压缩后处理。
TRANSIENT_ERRORS = (
    openai.RateLimitError,
    openai.InternalServerError,
    openai.APITimeoutError,
    openai.APIConnectionError,
)


def build_context_and_recovery_middleware(
    light_llm: BaseChatModel,
    *,
    summarize_trigger_tokens: int = 110_000,
    summarize_keep_messages: int = 20,
    clear_tool_results_trigger_tokens: int = 64_000,
    keep_tool_results: int = 3,
    tool_result_size_threshold: int = 200 * 1024,
    result_dir: Path = Path("/tmp/langchain_mem"),
    max_retries: int = 5,
    retry_initial_delay: float = 1.0,
    max_continuation_attempts: int = 2,
) -> list[AgentMiddleware]:
    """按成本从低到高组装上下文管线，再按从外到内组装模型调用的恢复链。

    上下文管线：
      工具返回时   ContextCompression.awrap_tool_call  超大结果落盘，只保留预览
      模型调用前   Summarization.before_model          超过阈值时 LLM 摘要替换 state 中的旧消息
      模型调用时   ContextEditing.wrap_model_call      清理较早的工具结果（只改本次请求）

    模型调用的 wrap 链（列表中越靠前越外层）：
      ModelFallback → ErrorRecovery → ModelRetry → 模型
      - ModelRetry 在最内层对临时故障做指数退避 + 抖动，重试用尽后抛出；
      - ErrorRecovery 处理上下文超限（兜底压缩后经 handler 重试）和输出截断（续写）；
      - ModelFallback 在最外层，主模型最终失败后换备用模型走同一条链。
    """
    compression = ContextCompressionMiddleware(
        llm=light_llm,
        tool_result_size_threshold=tool_result_size_threshold,
        result_dir=result_dir,
    )
    return [
        compression,
        SummarizationMiddleware(
            model=light_llm,
            trigger=("tokens", summarize_trigger_tokens),
            keep=("messages", summarize_keep_messages),
        ),
        ContextEditingMiddleware(
            edits=[ClearToolUsesEdit(trigger=clear_tool_results_trigger_tokens, keep=keep_tool_results)]
        ),
        ModelFallbackMiddleware(light_llm),
        ErrorRecoveryMiddleware(
            context_compressor=compression,
            max_continuation_attempts=max_continuation_attempts,
        ),
        ModelRetryMiddleware(
            max_retries=max_retries,
            retry_on=TRANSIENT_ERRORS,
            on_failure="error",
            initial_delay=retry_initial_delay,
        ),
    ]
