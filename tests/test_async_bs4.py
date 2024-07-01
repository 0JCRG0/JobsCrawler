import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
print(os.path.dirname(__file__), '..')
import asyncio
import logging
from src.models import AsyncBaseCrawl, Bs4Args


Bs4Args()
# Set up named logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

bs4_crawler = AsyncBaseCrawl(Bs4Args())

if __name__ == "__main__":
    asyncio.run(bs4_crawler.run())