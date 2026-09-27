from typing import List, Dict
from langchain_core.language_models import BaseChatModel
from langgraph.store.base import BaseStore
from langchain_core.messages import BaseMessage
import datetime
import json
import hashlib
import logging

logger = logging.getLogger(__name__)

EMPTY_EXTRACTION = {"semantic": [], "procedural": [], "episodic": []}


class MemorySaver:
    def __init__(self, llm: BaseChatModel, store: BaseStore):
        self.llm = llm
        self.store = store

    async def extract_and_save(
        self,
        namespace: tuple,
        messages: List[BaseMessage],
        existing_descriptions: List[str],
    ) -> None:
        """
        从本轮对话中提取三类记忆并保存到 store。

        Args:
            namespace: 当前用户的记忆命名空间
            messages: 本轮的用户消息与助手回复（调用方已排除工具输出）
            existing_descriptions: 已有记忆的描述，交给 LLM 避免重复提取
        """
        logger.info("MemorySaver.extract_and_save called")
        if not messages:
            return
        extracted = await self._extract_memories(messages, existing_descriptions)
        for mem_type, items in extracted.items():
            for item in items:
                await self._save_memory(namespace, mem_type, item)

    async def _extract_memories(
        self,
        messages: List[BaseMessage],
        existing_descriptions: List[str],
    ) -> Dict[str, List[Dict]]:
        """使用 LLM 从对话中提取三类记忆，返回结构化数据。"""
        logger.info("MemorySaver._extract_memories called")
        conversation = "\n".join([f"{m.type}: {m.content}" for m in messages])
        existing = "\n".join(f"- {d}" for d in existing_descriptions) or "（无）"

        prompt = f"""
你是一个记忆提取助手。请分析以下对话，提取关于**用户本人**的长期记忆，用于辅助未来的代码编写任务。

对话内容：
{conversation}

已有记忆（不要重复提取含义相同或相近的内容）：
{existing}

三类记忆定义：
1. **Semantic**（用户偏好）：用户个人的习惯、喜好、背景信息（例如编程语言偏好、代码风格、沟通方式等）。
2. **Procedural**（行为准则）：用户明示或暗示的工作流程、规则、规范（例如"提交前必须测试"、"不要使用第三方库"等）。
3. **Episodic**（过往经验）：用户经历过的具体事件、问题解决经验、项目背景（例如"上次部署时遇到端口冲突"）。

请按照如下 JSON 格式输出提取结果，若无则返回空列表：
{{
  "semantic": [{{"content": "...", "description": "..."}}],
  "procedural": [{{"content": "...", "description": "..."}}],
  "episodic": [{{"content": "...", "description": "..."}}]
}}

要求：
- 只提取用户本人表达或确认的信息。不要提取文件、代码、文档或助手解释中的知识性内容，这些内容不是关于用户的记忆。
- 每条记忆的 "content" 为完整描述（可保留细节），"description" 为 10-20 字的简短摘要（用于索引）。
- 与已有记忆重复的内容不要输出；没有值得长期记住的信息时，三个列表都返回空。
"""
        response = await self.llm.with_config(
            tags=["internal_memory_call"],
            metadata={"internal": True}
        ).ainvoke(prompt)
        # 响应可能是纯 JSON，也可能包在 ```json ... ``` 中
        content = response.content.strip() if response.content else ""
        if not content:
            return EMPTY_EXTRACTION
        if content.startswith("```json"):
            content = content[7:-3]
        elif content.startswith("```"):
            content = content[3:-3]
        try:
            data = json.loads(content)
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse memory JSON: {content[:200]}")
            return EMPTY_EXTRACTION
        return data

    async def _save_memory(self, namespace: tuple, mem_type: str, item: Dict):
        """保存单条记忆；描述完全相同的记忆直接跳过（语义去重由提取 prompt 负责）。"""
        key = hashlib.md5(item["description"].encode()).hexdigest()[:12]

        existing = await self.store.aget(namespace, key)
        if existing:
            logger.info(f"记忆已存在: {item['description']}")
            return

        await self.store.aput(
            namespace,
            key,
            {
                "type": mem_type,
                "content": item["content"],
                "description": item["description"],
                "timestamp": datetime.datetime.now().isoformat(),
            }
        )
