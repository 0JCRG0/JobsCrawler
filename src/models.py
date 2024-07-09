from dataclasses import dataclass
from typing import Any, TypeAlias
import aiohttp
import logging
import os
from collections.abc import Callable, Coroutine
from psycopg2.extensions import cursor
import pandas as pd
from src.crawlers.async_bs4 import async_bs4_crawl, clean_postgre_bs4
from src.crawlers.async_api import async_api_requests, clean_postgre_api
from src.crawlers.async_rss import async_rss_reader, clean_postgre_rss
from dotenv import load_dotenv

#TODO: Check types for each crawling function.

load_dotenv()

URL_DB = os.environ.get("URL_DB", "")

# Set up named logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


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
        Callable[[aiohttp.ClientSession], Coroutine[Any, Any, str]],
        aiohttp.ClientSession,
        Bs4Config | ApiConfig | RssConfig,
        cursor,
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
    custom_crawl_func: CustomCrawlFuncType = async_rss_reader
    custom_clean_func: Callable[[pd.DataFrame], pd.DataFrame] = clean_postgre_rss
    json_prod_path: str = rss_json_prod
    json_test_path: str = rss_json_test