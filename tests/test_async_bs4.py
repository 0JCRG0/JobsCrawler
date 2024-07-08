import sys
import os
import asyncio
import logging
#sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from dotenv import load_dotenv
from src.crawler import AsyncCrawlerEngine
from src.models import Bs4Args
# TODO: NEEDS ENV VARIABLES

load_dotenv()

LOGGER_PATH = os.environ.get("LOGGER_PATH")

logging.basicConfig(
    filename=LOGGER_PATH,
    level=logging.DEBUG,
    force=True,
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
)


async def main():
    bs4_engine = AsyncCrawlerEngine(Bs4Args())
    await bs4_engine.run()

if __name__ == "__main__":
    asyncio.run(main())




