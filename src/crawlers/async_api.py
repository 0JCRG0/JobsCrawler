#!/usr/local/bin/python3

import json
import pandas as pd
import os
import psycopg2
import random
import logging
from psycopg2.extensions import cursor, connection
from dotenv import load_dotenv
from src.models import ApiConfig
from src.utils.handy import USER_AGENTS, crawled_df_to_db
from src.utils.api_utils import class_json_strategy, clean_postgre_api, get_jobs_data
import asyncio
import aiohttp


# Load the environment variables
load_dotenv()

URL_DB = os.getenv("DATABASE_URL_DO", "")

api_resources_dir = os.path.join('src', 'resources', 'api_resources')
JSON_PROD = os.path.abspath(os.path.join(api_resources_dir, 'api_main.json'))
JSON_TEST = os.path.abspath(os.path.join(api_resources_dir, 'api_test.json'))

""" SET UP LOGGING FILE """

# Set up named logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class AsyncCrawlerBS4:
    def __init__(self, url_db=URL_DB):
        self.url_db = url_db
        self.conn: connection | None = None
        self.cur: cursor | None  = None

    async def __load_apis(self, json_data_path: str) -> list[ApiConfig]:
        with open(json_data_path) as f:
            data = json.load(f)
        return [
            ApiConfig(**api)
            for api in data[0]["apis"]
        ]

    async def __fetch(self, api_config: ApiConfig, session: aiohttp.ClientSession) -> str:
        random_user_agent = {"User-Agent": random.choice(USER_AGENTS)}
        async with session.get(api_config.api, headers=random_user_agent) as response:
            if response.status != 200:
                logging.warning(
                    f"Received non-200 response ({response.status}) for API: {api_config.api}. Skipping..."
                )
            logger.debug(f"random_header: {random_user_agent}")
            return await response.text()
        
    async def _async_endpoint_requests(self, session: aiohttp.ClientSession, api_config: ApiConfig, test: bool = False) -> dict[str, list[str]]:
        rows = {key: [] for key in ["title", "link", "description", "pubdate", "location", "timestamp"]}

        logger.info(f"{api_config.name} has started")
        logger.debug(f"All parameters for {api_config.name}:\n{api_config}")

        try:
            response = await self.__fetch(api_config, session)
            logger.debug(f"Successful request on {api_config.api}")
            data = json.loads(response)
            jobs = class_json_strategy(data, api_config)


            new_rows = await get_jobs_data(self.cur, jobs, session, api_config, test)
            if new_rows:
                for key in rows:
                    rows[key].extend(new_rows.get(key, []))
        # TODO: This exception should be better in the helper function
        except Exception as e:
            logger.error(f"{type(e).__name__} occurred before deploying crawling strategy on {api_config.api}.\n\n{e}", exc_info=True)
            pass
        return rows
    
    async def _gather_json_loads_api(self, session: aiohttp.ClientSession, test: bool = False) -> None:
        json_data_path = JSON_TEST if test else JSON_PROD
        api_configs = await self.__load_apis(json_data_path)

        tasks = [self._async_endpoint_requests(session, api_config, test) for api_config in api_configs]
        results = await asyncio.gather(*tasks)

        combined_data = {key: [] for key in ["title", "link", "description", "pubdate", "location", "timestamp"]}
        for result in results:
            for key in combined_data:
                combined_data[key].extend(result[key])

        lengths = {key: len(value) for key, value in combined_data.items()}
        if len(set(lengths.values())) == 1:
            df = clean_postgre_api(pd.DataFrame(combined_data))
            crawled_df_to_db(df, self.cur, test)
        else:
            logger.error(f"ERROR ON ASYNC BS4. LISTS DO NOT HAVE SAME LENGTH. FIX {lengths}")

    async def run(self, test: bool = False) -> None:
        start_time = asyncio.get_event_loop().time()

        self.conn = psycopg2.connect(self.url_db)
        self.cur = self.conn.cursor()

        async with aiohttp.ClientSession() as session:
            await self._gather_json_loads_api(session, test)

        self.conn.commit()
        self.cur.close()
        self.conn.close()

        elapsed_time = asyncio.get_event_loop().time() - start_time
        logger.info(f"Async API requests finished! all in: {elapsed_time:.2f} seconds.")
