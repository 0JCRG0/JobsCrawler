#!/usr/local/bin/python3

from collections.abc import Callable, Coroutine
from typing import Any
from bs4 import BeautifulSoup
import logging
from psycopg2.extensions import cursor
from src.extensions import Bs4Config
from src.utils.bs4_utils import (
    async_container_strategy_bs4,
    async_main_strategy_bs4,
    async_occ_mundial,
)
import aiohttp

# Set up named logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


async def __crawling_strategy(
    session: aiohttp.ClientSession,
    bs4_config: Bs4Config,
    soup: BeautifulSoup,
    test: bool,
    cur: cursor
) -> dict[str, list[str]] | None:
    strategy_map = {
        "main": async_main_strategy_bs4,
        "container": async_container_strategy_bs4,
        "occ": async_occ_mundial,
    }
    func_strategy = strategy_map.get(bs4_config.strategy)
    if not func_strategy:
        raise ValueError("Unrecognized strategy.")

    try:
        return await func_strategy(cur, session, bs4_config, soup, test)
    except Exception as e:
        logger.error(
            f"{type(e).__name__} using {bs4_config.strategy} strategy while crawling {bs4_config.url}.\n{e}",
            exc_info=True,
        )


async def async_bs4_crawl(
    fetch_func: Callable[[aiohttp.ClientSession], Coroutine[Any, Any, str]],
    session: aiohttp.ClientSession,
    bs4_config: Bs4Config,
    cur: cursor,
    test: bool = False,
) -> dict[str, list[str]]:
    rows = {
        key: []
        for key in ["title", "link", "description", "pubdate", "location", "timestamp"]
    }

    logger.info(f"{bs4_config.name} has started")
    logger.debug(f"All parameters for {bs4_config.name}:\n{bs4_config}")

    for i in range(bs4_config.start_point, bs4_config.pages_to_crawl + 1):
        url = bs4_config.url + str(i)

        try:
            html = await fetch_func(session)
            soup = BeautifulSoup(html, "lxml")
            logger.debug(f"Crawling {url} with {bs4_config.strategy} strategy")

            new_rows = await __crawling_strategy(session, bs4_config, soup, test, cur)
            if new_rows:
                for key in rows:
                    rows[key].extend(str(new_rows.get(key, [])))

        except Exception as e:
            logger.error(
                f"{type(e).__name__} occurred before deploying crawling strategy on {url}.\n\n{e}",
                exc_info=True,
            )
            continue
    return rows
