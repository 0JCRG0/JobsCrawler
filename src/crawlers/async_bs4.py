#!/usr/local/bin/python3

from collections.abc import Callable
from bs4 import BeautifulSoup
import logging
import os
from psycopg2.extensions import cursor
from dotenv import load_dotenv
from src.utils.bs4_utils import (
    async_container_strategy_bs4,
    async_main_strategy_bs4,
    async_occ_mundial,
)
import aiohttp
from src.models import Bs4Config

# Load the environment variables
load_dotenv()

URL_DB = os.getenv("DATABASE_URL_DO", "")

api_resources_dir = os.path.join("src", "resources", "bs4_resources")
JSON_PROD = os.path.abspath(os.path.join(api_resources_dir, "bs4_main.json"))
JSON_TEST = os.path.abspath(os.path.join(api_resources_dir, "bs4_test.json"))


LOGGER_PATH = os.path.join("logs", "main_logger.log")

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


async def __crawling_strategy(
    session: aiohttp.ClientSession,
    bs4_element: Bs4Config,
    soup: BeautifulSoup,
    test: bool,
    cur: cursor
) -> dict[str, list[str]] | None:
    strategy_map = {
        "main": async_main_strategy_bs4,
        "container": async_container_strategy_bs4,
        "occ": async_occ_mundial,
    }
    func_strategy = strategy_map.get(bs4_element.strategy)
    if not func_strategy:
        raise ValueError("Unrecognized strategy.")

    try:
        return await func_strategy(cur, session, bs4_element, soup, test)
    except Exception as e:
        logger.error(
            f"{type(e).__name__} using {bs4_element.strategy} strategy while crawling {bs4_element.url}.\n{e}",
            exc_info=True,
        )


async def async_bs4_crawl(
    fetch_func: Callable,
    session: aiohttp.ClientSession,
    bs4_element: Bs4Config,
    cur: cursor,
    test: bool = False,
) -> dict[str, list[str]]:
    rows = {
        key: []
        for key in ["title", "link", "description", "pubdate", "location", "timestamp"]
    }

    logger.info(f"{bs4_element.name} has started")
    logger.debug(f"All parameters for {bs4_element.name}:\n{bs4_element}")

    for i in range(bs4_element.start_point, bs4_element.pages_to_crawl + 1):
        url = bs4_element.url + str(i)

        try:
            html = await fetch_func(url, session)
            soup = BeautifulSoup(html, "lxml")
            logger.debug(f"Crawling {url} with {bs4_element.strategy} strategy")

            new_rows = await __crawling_strategy(session, bs4_element, soup, test, cur)
            if new_rows:
                for key in rows:
                    rows[key].extend(new_rows.get(key, []))

        except Exception as e:
            logger.error(
                f"{type(e).__name__} occurred before deploying crawling strategy on {url}.\n\n{e}",
                exc_info=True,
            )
            continue
    return rows
