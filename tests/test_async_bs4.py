from src.crawlers.async_bs4 import AsyncCrawlerBS4
import asyncio
import logging

# Set up named logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

bs4_crawler = AsyncCrawlerBS4()
asyncio.run(bs4_crawler.run(test=True))


