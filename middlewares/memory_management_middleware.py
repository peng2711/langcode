import datetime
from typing import List, Dict
from typing_extensions import NotRequired
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage
from langchain_core.language_models import BaseChatModel
from langgraph.config import get_config
from langgraph.store.base import BaseStore
from langchain.agents.middleware import AgentMiddleware, Runtime
from langchain.agents.middleware.types import AgentState
import logging

from middlewares.memory_saver import MemorySaver
from middlewares.context_vars import _internal_call

MEMORY_GUIDE = (
    "以下是关于当前用户的长期记忆，仅在与当前任务相关时参考。"
    "直接按这些信息行事，不要逐条复述记忆原文或类型标签。"
)

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

    def __init__(self, llm: BaseChatModel, store: BaseStore, default_user_id: str = "anonymous"):
        self.llm = llm
        self.store = store
        self.memory_saver = MemorySaver(llm, store)  # 复用 MemorySaver 的提取和保存逻辑
        self.default_user_id = default_user_id

    def _get_user_namespace(self) -> tuple:
        """按 config["configurable"]["user_id"] 隔离每个用户的记忆。"""
        user_id = get_config().get("configurable", {}).get("user_id") or self.default_user_id
        # Store 的 namespace 标签不能包含句点
        return (str(user_id).replace(".", "_"), "memories")

    @staticmethod
    def _dialogue(messages: List[BaseMessage]) -> List[BaseMessage]:
        """只保留用户消息和助手的文字回复，排除工具输出和发起工具调用的消息。"""
        return [
            m for m in messages
            if isinstance(m, HumanMessage)
            or (isinstance(m, AIMessage) and not m.tool_calls and isinstance(m.content, str) and m.content)
        ]

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
                # 不带 [SEMANTIC] 这类标签：带标签的条目看起来像要原样输出的数据，模型容易照抄
                parts.append(f"- {doc.value.get('content', '')}")
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

        recent_msgs = self._dialogue(messages)[-5:]
        recent_context = "\n".join([f"{m.type}: {m.content}" for m in recent_msgs])

        namespace = self._get_user_namespace()
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
        dynamic_content = f"{time_block}\n\n{MEMORY_GUIDE}\n{memory_text}"
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
        messages = state["messages"]
        # 只处理本轮：最后一条用户消息及之后的对话，之前轮次已经提取过
        turn_start = max(
            (i for i, m in enumerate(messages) if isinstance(m, HumanMessage)), default=0
        )
        namespace = self._get_user_namespace()
        index = await self._fetch_index(namespace, limit=200)
        await self.memory_saver.extract_and_save(
            namespace,
            self._dialogue(messages[turn_start:]),
            [item["description"] for item in index],
        )
        return None