# src/models.py
from dataclasses import dataclass
import psycopg2
from psycopg2.extensions import cursor, connection
import aiohttp
import asyncio
import logging
import json
import os
from dotenv import load_dotenv
import random
import pandas as pd
from collections.abc import Callable
from src.utils.bs4_utils import clean_postgre_bs4
from src.utils.rss_utils import clean_postgre_rss
from src.utils.api_utils import clean_postgre_api
from src.utils.handy import USER_AGENTS, crawled_df_to_db
from src.crawlers.async_bs4 import async_bs4_crawl

load_dotenv()

URL_DB = os.environ.get("DATABASE_URL_DO", "")
bs4_resources_dir = os.path.join("src", "resources", "bs4_resources")
api_resources_dir = os.path.join("src", "resources", "api_resources")
rss_resources_dir = os.path.join("src", "resources", "rss_resources")


bs4_json_prod = os.path.abspath(os.path.join(bs4_resources_dir, "bs4_main.json"))
bs4_json_test = os.path.abspath(os.path.join(bs4_resources_dir, "bs4_test.json"))

api_json_prod = os.path.abspath(os.path.join(api_resources_dir, "api_main.json"))
api_json_test = os.path.abspath(os.path.join(api_resources_dir, "api_test.json"))

rss_json_prod = os.path.abspath(os.path.join(rss_resources_dir, "api_main.json"))
rss_json_test = os.path.abspath(os.path.join(rss_resources_dir, "api_test.json"))

# Set up named logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@dataclass
class Bs4ElementPath:
    jobs_path: str
    title_path: str
    link_path: str
    location_path: str
    description_path: str


@dataclass
class Bs4Config:
    name: str
    url: str
    pages_to_crawl: int
    start_point: int
    strategy: str
    follow_link: str
    inner_link_tag: str
    elements_path: Bs4ElementPath

@dataclass
class Bs4Args:
    config: type[Bs4Config] = Bs4Config
    custom_crawl_func: Callable = async_bs4_crawl
    test: bool = False
    json_prod_path: str = bs4_json_prod
    json_test_path: str = bs4_json_test
    url_db: str = URL_DB


@dataclass
class ApiElementPath:
    dict_tag: str
    title_tag: str
    link_tag: str
    description_tag: str
    pubdate_tag: str
    location_tag: str
    location_default: str


@dataclass
class ApiConfig:
    name: str
    url: str
    class_json: int
    follow_link: str
    inner_link_tag: str
    elements_path: ApiElementPath

@dataclass
class ApiArgs:
    config: type[ApiConfig] = ApiConfig
    custom_crawl_func: Callable = async_api_crawl
    test: bool = False
    json_prod_path: str = api_json_prod
    json_test_path: str = api_json_test
    url_db: str = URL_DB

@dataclass
class RssConfig:
    url: str
    title_tag: str
    link_tag: str
    description_tag: str
    location_tag: str
    follow_link: str
    inner_link_tag: str

@dataclass
class RssArgs:
    config: type[RssConfig] = RssConfig
    custom_crawl_func: Callable = async_rss_crawl
    test: bool = False
    json_prod_path: str = rss_json_prod
    json_test_path: str = rss_json_test
    url_db: str = URL_DB


class AsyncBaseCrawl:
    def __init__(
        self, args: Bs4Args | ApiArgs | RssArgs) -> None:
        self.config: type[Bs4Config] | type[ApiConfig] | type[RssConfig] = args.config
        self.test: bool = args.test
        self.json_data_path: str = args.json_test_path if self.test else args.json_prod_path
        self.custom_crawl_func: Callable = args.custom_crawl_func
        self.url_db: str = args.url_db
        self.conn: connection | None = None
        self.cur: cursor | None = None

    async def __load_urls(self) -> list[Bs4Config | RssConfig | ApiConfig]:
        with open(self.json_data_path) as f:
            data = json.load(f)
        return [self.config(**url) for url in data]

    async def __fetch(
        self, session: aiohttp.ClientSession
    ) -> str:
        random_user_agent = {"User-Agent": random.choice(USER_AGENTS)}
        async with session.get(self.config.url, headers=random_user_agent) as response:
            if response.status != 200:
                logging.warning(
                    f"Received non-200 response ({response.status}) for API: {self.config.url}. Skipping..."
                )
                pass
            logger.debug(f"random_header: {random_user_agent}")
            return await response.text()

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

        crawling_configs = await self.__load_urls()

        tasks = [
            self.custom_crawl_func(self.__fetch, session, crawling_config, self.test, self.cur)
            for crawling_config in crawling_configs
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
