import asyncio
from collections.abc import Coroutine
from typing import Any

from src.crawler import AsyncCrawlerEngine
from src.models import ApiArgs, Bs4Args, RssArgs
from src.utils.logger_helper import get_custom_logger

logger = get_custom_logger(__name__)

async def run_strategy(args: RssArgs | ApiArgs | Bs4Args) -> Coroutine[Any, Any, None] | None:
    engine = AsyncCrawlerEngine(args)
    await engine.run()

async def run_crawlers(is_test: bool) -> Coroutine[Any, Any, None] | None:
    if not is_test:
        raise ValueError("The 'is_test' argument must be True as this is a test.")
    start_time = asyncio.get_event_loop().time()

    strategies = [
        (RssArgs(test=is_test)),
        (ApiArgs(test=is_test)),
        (Bs4Args(test=is_test)),
    ]

    tasks = [run_strategy(args) for args in strategies]

    try:
        await asyncio.gather(*tasks)
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")

    elapsed_time = asyncio.get_event_loop().time() - start_time
    logger.info(f"All strategies completed in {elapsed_time:.2f} seconds")

if __name__ == "__main__":
	asyncio.run(run_crawlers(True))