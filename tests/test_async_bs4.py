#from src.crawlers.async_bs4 import AsyncCrawlerBS4
import asyncio
import logging
import os
from src.models import AsyncBaseCrawl, Bs4Args

Bs4Args()
# Set up named logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

bs4_crawler = AsyncBaseCrawl(Bs4Args())

asyncio.run(bs4_crawler.run())




