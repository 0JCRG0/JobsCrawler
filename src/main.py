import sys
import os
import asyncio
import logging
from typing import Any, Coroutine
from concurrent.futures import ThreadPoolExecutor
from crawler import AsyncCrawlerEngine
from embeddings.embed_latest_crawled_data import embed_data
from models import RssArgs, ApiArgs, Bs4Args
DB_URL = os.environ.get("URL_DB")
if not DB_URL:
    error_msg = "The environmental variable DB_URL is empty."
    logging.error(error_msg)
    raise ValueError(error_msg)

LOGGER_PATH = os.path.join("logs", "main_logger.log")
(
    os.makedirs(os.path.dirname(LOGGER_PATH), exist_ok=True)
    if not os.path.exists(LOGGER_PATH)
    else None
)
open(LOGGER_PATH, "a").close() if not os.path.exists(LOGGER_PATH) else None


logging.basicConfig(
    filename=LOGGER_PATH,
    level=logging.INFO,
    force=True,
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
)

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
        logging.error(f"An error occurred: {str(e)}")

    elapsed_time = asyncio.get_event_loop().time() - start_time
    logging.info(f"All strategies completed in {elapsed_time:.2f} seconds")
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