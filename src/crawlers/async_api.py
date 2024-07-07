#!/usr/local/bin/python3
from collections.abc import Callable, Coroutine
import json
import logging
from typing import Any
from psycopg2.extensions import cursor
from dotenv import load_dotenv
from types_definitions import ApiConfig
from src.utils.api_utils import class_json_strategy, get_jobs_data
import aiohttp


# Load the environment variables
load_dotenv()

""" SET UP LOGGING FILE """


# Set up named logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


async def async_api_requests(
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
    if not isinstance(api_config, ApiConfig):
        error_msg = f"The provided config does not have the expected type. Expected: {ApiConfig}. Received: {api_config}."
        logger.error(error_msg)
        raise ValueError(error_msg)

    try:
        response = await fetch_func(session)
        logger.debug(f"Successful request on {api_config.url}")
        data = json.loads(response)
        jobs = class_json_strategy(data, api_config)

        new_rows = await get_jobs_data(cur, jobs, session, api_config, test)
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