import asyncio

from src.crawler import AsyncCrawlerEngine
from src.models import ApiArgs
from src.utils.logger_helper import get_custom_logger

logger = get_custom_logger(__name__)

async def main(is_test: bool = True):
    if not is_test:
        raise ValueError("The 'is_test' argument must be True as this is a test.")    
    logger.info("Testing Async API crawler")
    api_engine = AsyncCrawlerEngine(ApiArgs(test=True))
    await api_engine.run()

if __name__ == "__main__":
    asyncio.run(main(True))




