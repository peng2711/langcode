import datetime
from typing import List, Dict
from typing_extensions import NotRequired
from langchain_core.messages import AIMessage, SystemMessage
from langchain_core.language_models import BaseChatModel
from langgraph.store.base import BaseStore
from langchain.agents.middleware import AgentMiddleware, Runtime
from langchain.agents.middleware.types import AgentState
import logging

from middlewares.memory_saver import MemorySaver
from middlewares.context_vars import _internal_call

logger = logging.getLogger(__name__)


class MemoryState(AgentState):
    recalled_memories: NotRequired[str]


class MemoryManagementMiddleware(AgentMiddleware):
    """
    管理长期记忆的召回与注入。
    使用 LLM 从索引（description 列表）中选择最相关记忆，而非向量检索。

    召回在每轮用户输入开始时（before_agent）执行一次，结果缓存在 state 中；
    同一轮内的每次模型调用（wrap_model_call）只读取缓存注入 system prompt，
    不会在工具调用循环的每一步都重复调用 LLM 挑选记忆。
    """
    state_schema = MemoryState

    def __init__(self, llm: BaseChatModel, store: BaseStore, user_id: str = "user_id"):
        self.llm = llm
        self.store = store
        self.memory_saver = MemorySaver(llm, store, user_id)  # 复用 MemorySaver 的提取和保存逻辑
        self.user_id_key = user_id

    def _get_user_namespace(self, state: AgentState) -> tuple:
        return (self.user_id_key, "memories")

    async def _fetch_index(self, namespace: tuple, limit: int = 200) -> List[Dict[str, str]]:
        """从 store 中获取所有记忆的 description 和 key（最多 limit 条）"""
        items = await self.store.asearch(namespace, limit=limit)
        index = []
        for item in items:
            if item.value and "description" in item.value:
                index.append({
                    "key": item.key,
                    "description": item.value["description"],
                    "type": item.value.get("type", "contextual")
                })
        return index

    async def _select_relevant_keys(self, task: str, recent_context: str, index: List[Dict]) -> List[str]:
        """调用 LLM 选择最相关的记忆 key（最多 5 个）"""
        logger.info("MemoryManagementMiddleware._select_relevant_keys called")
        if not index:
            return []

        # 构建索引文本（限制每个描述的长度，防止过长）
        index_text = "\n".join([
            f"- {item['description']} (key: {item['key']})"
            for item in index
        ])

        prompt = f"""你是一个记忆检索助手。根据最近的对话，从以下记忆列表中选择最多 5 个最相关的记忆。

用户最新提问：
{task[:500]}

最近的对话：
{recent_context[:2000]}

记忆列表（每项包含描述和对应的 key）：
{index_text}

请只返回选中的 key 列表，用英文逗号分隔，不要包含其他内容。
例如：key1, key3, key7
"""
        token = _internal_call.set(True)
        try:
            response = await self.llm.with_config(
                callbacks=[],
                tags=["internal_memory_call"],
                metadata={"internal": True}
            ).ainvoke(prompt)
        finally:
            _internal_call.reset(token)
        # 解析响应
        raw_keys = [k.strip() for k in response.content.split(',') if k.strip()]
        # 去重并限制 ≤5
        unique_keys = list(dict.fromkeys(raw_keys))
        return unique_keys[:5]

    async def _load_memories(self, namespace: tuple, keys: List[str]) -> str:
        """根据 key 从 store 加载完整内容，并拼接成文本"""
        if not keys:
            return ""
        parts = []
        for key in keys:
            doc = await self.store.aget(namespace, key)
            if doc:
                mem_type = doc.value.get("type", "contextual")
                content = doc.value.get("content", "")
                parts.append(f"[{mem_type.upper()}] {content}")
        return "\n".join(parts)

    async def abefore_agent(
        self,
        state: AgentState,
        runtime: Runtime,
    ) -> dict[str, any] | None:
        """每轮用户输入召回一次记忆，写入 state["recalled_memories"]。"""
        logger.info("MemoryManagementMiddleware.abefore_agent called")
        return {"recalled_memories": await self._recall(state)}

    async def _recall(self, state: AgentState) -> str:
        messages = state.get("messages", [])
        task = ""
        if messages and messages[-1].type == "human":
            task = messages[-1].content
            # 用户明确要求不使用记忆
            if any(kw in task.lower() for kw in ["不要使用记忆", "禁用记忆", "忘记所有", "停止记忆"]):
                return ""

        recent_msgs = [m for m in messages[-6:] if m.type != "system"][-5:]
        recent_context = "\n".join([f"{m.type}: {m.content}" for m in recent_msgs])

        namespace = self._get_user_namespace(state)
        index = await self._fetch_index(namespace, limit=200)
        if not index:
            return ""

        relevant_keys = await self._select_relevant_keys(task, recent_context, index)
        return await self._load_memories(namespace, relevant_keys)

    async def awrap_model_call(self, request, handler):
        """把本轮召回的记忆和当前时间追加到 system prompt。"""
        memory_text = request.state.get("recalled_memories", "")
        if not memory_text:
            return await handler(request)

        time_block = f"Current time: {datetime.datetime.now().isoformat(timespec='seconds')}"
        dynamic_content = f"{time_block}\n\nAvailable memories:\n{memory_text}"
        system_message = request.system_message
        if system_message is None:
            system_message = SystemMessage(content=dynamic_content)
        else:
            system_message = SystemMessage(
                content_blocks=[*system_message.content_blocks, {"type": "text", "text": dynamic_content}]
            )
        return await handler(request.override(system_message=system_message))

    async def aafter_model(
        self,
        state: AgentState,
        runtime: Runtime,
    ) -> dict[str, any] | None:
        last_message=state["messages"][-1] if state["messages"] else None
        # 只有当模型回复且未调用工具时才进行记忆提取和保存，避免工具调用结果干扰记忆内容
        if not isinstance(last_message, AIMessage):
            return None
        if bool(getattr(last_message, 'tool_calls', [])):
            return None
        logger.info("MemoryManagementMiddleware.aafter_model called")
        await self.memory_saver.extract_and_save(state["messages"])
        return None