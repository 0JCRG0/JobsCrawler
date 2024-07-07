from dataclasses import dataclass
from typing import Any, TypeAlias
import aiohttp
import logging
import os
from collections.abc import Callable, Coroutine
import psycopg2
from psycopg2.extensions import cursor, connection
import asyncio
import json
import random
import pandas as pd
from src.utils.bs4_utils import clean_postgre_bs4
from src.utils.rss_utils import clean_postgre_rss
from src.utils.api_utils import clean_postgre_api
from src.crawlers.async_bs4 import async_bs4_crawl
from src.crawlers.async_api import async_api_requests
from src.utils.handy import crawled_df_to_db
from src.constants import LOGGER_PATH, USER_AGENTS, URL_DB

logging.basicConfig(
    filename=LOGGER_PATH,
    level=logging.DEBUG,
    force=True,
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Get the paths of the JSON files
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
class RssConfig:
    url: str
    title_tag: str
    link_tag: str
    description_tag: str
    location_tag: str
    follow_link: str
    inner_link_tag: str


CustomCrawlFuncType: TypeAlias = Callable[
    [
        Callable[
            [aiohttp.ClientSession], Coroutine[Any, Any, Coroutine[Any, Any, str]]
        ],
        aiohttp.ClientSession,
        Bs4Config | ApiConfig | RssConfig,
        cursor | None,
        bool,
    ],
    Coroutine[Any, Any, dict[str, list[str]]],
]


@dataclass
class BaseArgs:
    config: type
    custom_crawl_func: CustomCrawlFuncType
    custom_clean_func: Callable[[pd.DataFrame], pd.DataFrame]
    test: bool = False
    url_db: str = URL_DB
    json_prod_path: str = ""
    json_test_path: str = ""


@dataclass
class Bs4Args(BaseArgs):
    config: type[Bs4Config] = Bs4Config
    custom_crawl_func: CustomCrawlFuncType = async_bs4_crawl
    custom_clean_func: Callable[[pd.DataFrame], pd.DataFrame] = clean_postgre_bs4
    json_prod_path: str = bs4_json_prod
    json_test_path: str = bs4_json_test


@dataclass
class ApiArgs(BaseArgs):
    config: type[ApiConfig] = ApiConfig
    custom_crawl_func: CustomCrawlFuncType = async_api_requests
    custom_clean_func: Callable[[pd.DataFrame], pd.DataFrame] = clean_postgre_api
    json_prod_path: str = api_json_prod
    json_test_path: str = api_json_test


@dataclass
class RssArgs(BaseArgs):
    config: type[RssConfig] = RssConfig
    custom_crawl_func: CustomCrawlFuncType = async_rss_crawl
    custom_clean_func: Callable[[pd.DataFrame], pd.DataFrame] = clean_postgre_rss
    json_prod_path: str = rss_json_prod
    json_test_path: str = rss_json_test


class AsyncCrawlerEngine:
    def __init__(self, args: BaseArgs) -> None:
        self.config = args.config
        self.test = args.test
        self.json_data_path = args.json_test_path if self.test else args.json_prod_path
        self.custom_crawl_func = args.custom_crawl_func
        self.custom_clean_func = args.custom_clean_func
        self.url_db = args.url_db
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

        configs = await self.__load_configs()

        tasks = [
            self.custom_crawl_func(
                self.__fetch,
                session,
                config,
                self.cur,
                self.test,
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
            df = self.custom_clean_func(pd.DataFrame(combined_data))
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
