"""Start one generic Agent Runtime process.

Example:
    python -m worker --runtime-id runtime-001
"""

from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from lib.dag_scheduler import DAGScheduler
from lib.db import create_async_pool
from lib.message_hub import OutboxDispatcher, RocketMQMessageBus
from lib.sub_agent import run_sub_agent
from lib.structured_logging import configure_structured_logging


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a generic LangCode Runtime")
    parser.add_argument("--runtime-id", required=True)
    parser.add_argument("--runtime-version", default="v1")
    parser.add_argument("--max-rounds", type=int, default=30)
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    load_dotenv(override=True)
    configure_structured_logging("langcode-runtime")

    pool = create_async_pool()
    await pool.open()
    try:
        scheduler = DAGScheduler(pool)
        await scheduler.setup()
        checkpointer = AsyncPostgresSaver(pool)
        await checkpointer.setup()

        llm = ChatOpenAI(
            model=os.getenv("MODEL_NAME"),
            api_key=os.getenv("API_KEY"),
            base_url=os.getenv("BASE_URL"),
            temperature=0.3,
            max_completion_tokens=8000,
        )
        light_llm = ChatOpenAI(
            model=os.getenv("LIGHT_MODEL_NAME"),
            api_key=os.getenv("API_KEY"),
            base_url=os.getenv("BASE_URL"),
            temperature=0.3,
            max_completion_tokens=8000,
            verbose=False,
        )
        message_bus = RocketMQMessageBus(pool)
        await message_bus.setup()
        dispatcher_task = asyncio.create_task(
            OutboxDispatcher(pool, message_bus).run()
        )
        try:
            await run_sub_agent(
                runtime_id=args.runtime_id,
                runtime_version=args.runtime_version,
                llm=llm,
                checkpointer=checkpointer,
                message_bus=message_bus,
                work_dir=Path.cwd(),
                light_llm=light_llm,
                max_rounds=args.max_rounds,
            )
        finally:
            dispatcher_task.cancel()
            try:
                await dispatcher_task
            except asyncio.CancelledError:
                pass
            await message_bus.close()
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
