from typing import Any
import psycopg2
from psycopg2.extensions import cursor, connection
import aiohttp
import asyncio
import logging
import json
import random
import pandas as pd
from collections.abc import Callable, Coroutine
from src.utils.bs4_utils import clean_postgre_bs4
from src.utils.rss_utils import clean_postgre_rss
from src.utils.api_utils import clean_postgre_api
from src.utils.handy import crawled_df_to_db
from src.models import Bs4Args, ApiArgs, RssArgs, Bs4Config, ApiConfig, RssConfig
from src.constants import LOGGER_PATH, USER_AGENTS

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

class AsyncBaseCrawl:
    def __init__(self, args: Bs4Args | ApiArgs | RssArgs) -> None:
        self.config: type[Bs4Config] | type[ApiConfig] | type[RssConfig] = args.config
        self.test: bool = args.test
        self.json_data_path: str = (
            args.json_test_path if self.test else args.json_prod_path
        )
        self.custom_crawl_func: Callable = args.custom_crawl_func
        self.url_db: str = args.url_db
        self.conn: connection | None = None
        self.cur: cursor | None = None

    async def __load_configs(self) -> list[Bs4Config | RssConfig | ApiConfig]:
        with open(self.json_data_path) as f:
            data = json.load(f)
        return [self.config(**url) for url in data]

    async def __fetch(self, session: aiohttp.ClientSession) -> Coroutine[Any, Any, str]:
        random_user_agent = {"User-Agent": random.choice(USER_AGENTS)}
        async with session.get(self.config.url, headers=random_user_agent) as response:
            if response.status != 200:
                logging.warning(
                    f"Received non-200 response ({response.status}) for API: {self.config.url}. Skipping..."
                )
                pass
            logger.debug(f"random_header: {random_user_agent}")
            return response.text()

    async def __gather_json_loads(
        self,
        session: aiohttp.ClientSession,
    ) -> None:
        cleaning_map = {
            "Bs4Config": clean_postgre_bs4,
            "ApiConfig": clean_postgre_api,
            "RssConfig": clean_postgre_rss,
        }
        func_cleaning = cleaning_map.get(type(self.config).__name__)
        if not func_cleaning:
            raise ValueError("Unrecognized cleaning function.")

        configs = await self.__load_configs()

        tasks = [
            self.custom_crawl_func(
                self.__fetch, session, config, self.test, self.cur
            )
            for config in configs
        ]
        results = await asyncio.gather(*tasks)

        combined_data = {
            key: []
            for key in [
                "title",
                "link",
                "description",
                "pubdate",
                "location",
                "timestamp",
            ]
        }
        for result in results:
            for key in combined_data:
                combined_data[key].extend(result[key])

        lengths = {key: len(value) for key, value in combined_data.items()}
        if len(set(lengths.values())) == 1:
            df = func_cleaning(pd.DataFrame(combined_data))
            crawled_df_to_db(df, self.cur, self.test)
        else:
            logger.error(
                f"ERROR ON {type(self.config).__name__}. LISTS DO NOT HAVE SAME LENGTH. FIX {lengths}"
            )

    async def run(self) -> None:
        start_time = asyncio.get_event_loop().time()

        self.conn = psycopg2.connect(self.url_db)
        self.cur = self.conn.cursor()

        async with aiohttp.ClientSession() as session:
            await self.__gather_json_loads(session)

        self.conn.commit()
        self.cur.close()
        self.conn.close()

        elapsed_time = asyncio.get_event_loop().time() - start_time
        logger.info(f"Async BS4 crawlers finished! all in: {elapsed_time:.2f} seconds.")
