import sys
import os
import asyncio
from utils.logger_helper import get_custom_logger
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.crawler import AsyncCrawlerEngine
from src.models import ApiArgs

logger = get_custom_logger(__name__)

async def main(is_test: bool = True):
    if not is_test:
        raise ValueError("The 'is_test' argument must be True as this is a test.")    
    api_engine = AsyncCrawlerEngine(ApiArgs(test=True))
    await api_engine.run()

if __name__ == "__main__":
    asyncio.run(main(True))




