#!/usr/local/bin/python3

from collections.abc import Callable, Coroutine
from typing import Any
import feedparser
import logging
from psycopg2.extensions import cursor
from models import ApiConfig
import aiohttp
from src.utils.rss_utils import async_get_feed_entries


# Set up named logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

async def async_rss_reader(
    fetch_func: Callable[[aiohttp.ClientSession], Coroutine[Any, Any, str]],
    session: aiohttp.ClientSession,
    api_config: ApiConfig,
    cur: cursor,
    test: bool = False,
) -> dict[str, list[str]]:
    rows = {
        key: []
        for key in ["title", "link", "description", "pubdate", "location", "timestamp"]
    }

    logger.info(f"{api_config.name} has started")
    logger.debug(f"All parameters for {api_config.name}:\n{api_config}")

    try:
        response = await fetch_func(session)
        logger.debug(f"Successful request on {api_config.url}")
        feed = feedparser.parse(response)

        new_rows = await async_get_feed_entries(cur, jobs, session, api_config, test)
        if new_rows:
            for key in rows:
                rows[key].extend(new_rows.get(key, []))
    except Exception as e:
        logger.error(
            f"{type(e).__name__} occurred before deploying crawling strategy on {api_config.url}.\n\n{e}",
            exc_info=True,
        )
        pass
    return rows
