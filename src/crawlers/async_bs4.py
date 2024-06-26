#!/usr/local/bin/python3

from bs4 import BeautifulSoup
import pandas as pd
import json
import logging
import os
import psycopg2
from psycopg2.extensions import cursor, connection
from dotenv import load_dotenv
from src.utils.handy import USER_AGENTS, crawled_df_to_db
from src.utils.bs4_utils import (
    async_container_strategy_bs4,
    async_main_strategy_bs4,
    async_occ_mundial,
    clean_postgre_bs4,
)
import asyncio
import random
import aiohttp
from src.models import Bs4Element

# Load the environment variables
load_dotenv()

URL_DB = os.getenv("DATABASE_URL_DO", "")

api_resources_dir = os.path.join('src', 'resources', 'bs4_resources')
JSON_PROD = os.path.abspath(os.path.join(api_resources_dir, 'bs4_main.json'))
JSON_TEST = os.path.abspath(os.path.join(api_resources_dir, 'bs4_test.json'))


LOGGER_PATH = os.path.join('logs', 'main_logger.log')

logging.basicConfig(
    filename=LOGGER_PATH,
    level=logging.DEBUG,
    force=True,
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Set up named logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class AsyncCrawlerBS4:
    def __init__(self, url_db=URL_DB):
        self.url_db = url_db
        self.conn: connection | None = None
        self.cur: cursor | None  = None

    async def __load_urls(self, json_data_path: str) -> list[Bs4Element]:
        with open(json_data_path) as f:
            data = json.load(f)
        return [
            Bs4Element(**url)
            for url in data[0]["urls"]
        ]

    async def __fetch(self, url: str, session: aiohttp.ClientSession) -> str:
        random_user_agent = {"User-Agent": random.choice(USER_AGENTS)}
        async with session.get(url, headers=random_user_agent) as response:
            logger.debug(f"random_header: {random_user_agent}")
            return await response.text()

    async def __crawling_strategy(self, session: aiohttp.ClientSession, bs4_element: Bs4Element, soup, test) -> dict[str, list[str]] | None:
        strategy_map = {
            "main": async_main_strategy_bs4,
            "container": async_container_strategy_bs4,
            "occ": async_occ_mundial,
        }
        func_strategy = strategy_map.get(bs4_element.strategy)
        if not func_strategy:
            raise ValueError("Unrecognized strategy.")

        try:
            return await func_strategy(self.cur, session, bs4_element, soup, test)
        except Exception as e:
            logger.error(f"{type(e).__name__} using {bs4_element.strategy} strategy while crawling {bs4_element.url}.\n{e}", exc_info=True)

    async def _async_bs4_crawler(self, session: aiohttp.ClientSession, bs4_element: Bs4Element, test: bool = False) -> dict[str, list[str]]:
        rows = {key: [] for key in ["title", "link", "description", "pubdate", "location", "timestamp"]}

        logger.info(f"{bs4_element.name} has started")
        logger.debug(f"All parameters for {bs4_element.name}:\n{bs4_element}")

        for i in range(bs4_element.start_point, bs4_element.pages_to_crawl + 1):
            url = bs4_element.url + str(i)

            try:
                html = await self.__fetch(url, session)
                soup = BeautifulSoup(html, "lxml")
                logger.debug(f"Crawling {url} with {bs4_element.strategy} strategy")

                new_rows = await self.__crawling_strategy(session, bs4_element, soup, test)
                if new_rows:
                    for key in rows:
                        rows[key].extend(new_rows.get(key, []))

            except Exception as e:
                logger.error(f"{type(e).__name__} occurred before deploying crawling strategy on {url}.\n\n{e}", exc_info=True)
                continue
        return rows

    async def _gather_json_loads_bs4(self, session: aiohttp.ClientSession, test: bool = False) -> None:
        json_data_path = JSON_TEST if test else JSON_PROD
        bs4_elements = await self.__load_urls(json_data_path)

        tasks = [self._async_bs4_crawler(session, bs4_element, test) for bs4_element in bs4_elements]
        results = await asyncio.gather(*tasks)

        combined_data = {key: [] for key in ["title", "link", "description", "pubdate", "location", "timestamp"]}
        for result in results:
            for key in combined_data:
                combined_data[key].extend(result[key])

        lengths = {key: len(value) for key, value in combined_data.items()}
        if len(set(lengths.values())) == 1:
            df = clean_postgre_bs4(pd.DataFrame(combined_data))
            crawled_df_to_db(df, self.cur, test)
        else:
            logger.error(f"ERROR ON ASYNC BS4. LISTS DO NOT HAVE SAME LENGTH. FIX {lengths}")

    async def run(self, test: bool = False) -> None:
        start_time = asyncio.get_event_loop().time()

        self.conn = psycopg2.connect(self.url_db)
        self.cur = self.conn.cursor()

        async with aiohttp.ClientSession() as session:
            await self._gather_json_loads_bs4(session, test)

        self.conn.commit()
        self.cur.close()
        self.conn.close()

        elapsed_time = asyncio.get_event_loop().time() - start_time
        logger.info(f"Async BS4 crawlers finished! all in: {elapsed_time:.2f} seconds.")



