import asyncio
import logging
import os
import sys
import traceback
from typing import Any, Coroutine

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.crawler import AsyncCrawlerEngine
from src.embeddings.embed_latest_crawled_data import embed_data
from src.models import ApiArgs, Bs4Args, RssArgs

LOGGER_PATH = os.path.join("logs", "main_logger.log")
(
    os.makedirs(os.path.dirname(LOGGER_PATH), exist_ok=True)
    if not os.path.exists(LOGGER_PATH)
    else None
)
open(LOGGER_PATH, "a").close() if not os.path.exists(LOGGER_PATH) else None


logging.basicConfig(
    filename=LOGGER_PATH,
    level=logging.DEBUG,
    force=True,
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
)

async def run_strategy(args: RssArgs | ApiArgs | Bs4Args) -> Coroutine[Any, Any, None] | None:
    engine = AsyncCrawlerEngine(args)
    await engine.run()

async def run_crawlers(is_test: bool = True) -> Coroutine[Any, Any, None] | None:
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
        logging.error(f"An error occurred: {str(e)}. ")
        logging.error("Full traceback:")
        logging.error(traceback.format_exc())

    elapsed_time = asyncio.get_event_loop().time() - start_time
    logging.info(f"All strategies completed in {elapsed_time:.2f} seconds")
async def main():
	await run_crawlers(True)
	await asyncio.to_thread(embed_data, embedding_model="e5_base_v2", test=True)

if __name__ == "__main__":
	asyncio.run(main())