import os
import asyncio
from utils.logger_helper import get_custom_logger
from typing import Any, Coroutine
from concurrent.futures import ThreadPoolExecutor
from crawler import AsyncCrawlerEngine
from embeddings.embed_latest_crawled_data import embed_data
from models import RssArgs, ApiArgs, Bs4Args
DB_URL = os.environ.get("URL_DB")

logger = get_custom_logger(__name__)

if not DB_URL:
    error_msg = "The environmental variable DB_URL is empty."
    logger.error(error_msg)
    raise ValueError(error_msg)

async def run_strategy(args: RssArgs | ApiArgs | Bs4Args) -> Coroutine[Any, Any, None] | None:
    engine = AsyncCrawlerEngine(args)
    await engine.run()

async def run_crawlers(is_test: bool = False) -> Coroutine[Any, Any, None] | None:
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
    print(f"All strategies completed in {elapsed_time:.2f} seconds")

async def main():
    # Run crawlers and wait for them to complete
    await run_crawlers()
    
    # Now run embed_data in a separate thread
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as pool:
        await loop.run_in_executor(pool, embed_data, "e5_base_v2")


if __name__ == "__main__":
	asyncio.run(main())