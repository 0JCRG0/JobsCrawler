import sys
import os
import asyncio
import logging
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.crawler import AsyncCrawlerEngine
from src.models import ApiArgs

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

async def main(is_test: bool = True):
    if not is_test:
        raise ValueError("The 'is_test' argument must be True as this is a test.")    
    api_engine = AsyncCrawlerEngine(ApiArgs(test=True))
    await api_engine.run()

if __name__ == "__main__":
    asyncio.run(main(True))




