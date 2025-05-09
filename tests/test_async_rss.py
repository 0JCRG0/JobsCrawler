import asyncio

from src.crawler import AsyncCrawlerEngine
from src.models import RssArgs
from src.utils.logger_helper import get_custom_logger

logger = get_custom_logger(__name__)

async def main(is_test: bool = True):
    if not is_test:
        raise ValueError("The 'is_test' argument must be True as this is a test.")
    rss_engine = AsyncCrawlerEngine(RssArgs(test=True))
    await rss_engine.run()

if __name__ == "__main__":
    asyncio.run(main(True))




