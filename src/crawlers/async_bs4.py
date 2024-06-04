#!/usr/local/bin/python3

import bs4
import pandas as pd
import pretty_errors  # noqa: F401
import json
import logging
import os
import psycopg2
from dotenv import load_dotenv
from utils.handy import USER_AGENTS, setup_main_logger, crawled_df_to_db
from utils.bs4_utils import (
    async_container_strategy_bs4,
    async_main_strategy_bs4,
    async_occ_mundial,
    clean_postgre_bs4,
)
import asyncio
import random
import aiohttp

""" LOAD THE ENVIRONMENT VARIABLES """
load_dotenv()

JSON_PROD = os.environ.get("JSON_PROD_BS4", "")
LOGGER_PATH = os.environ.get("LOGGER_PATH", "")
URL_DB = os.environ.get("DATABASE_URL_DO", "")


""" SET UP LOGGING FILE """
current_dir = os.path.dirname(os.path.abspath(__file__))

logging_file_path = os.path.join(current_dir, LOGGER_PATH)

setup_main_logger(logging_file_path)


""" CONNECTION & DATA """

JSON = os.path.join(current_dir, JSON_PROD)
CONN = psycopg2.connect(URL_DB)
CUR = CONN.cursor()

#TODO: Each site to crawl should be a data class or pylance base class.
#: dict[str, str | list[dict[str, str]]]

"""
Instead of passing this. You can pass the class:

elements_path,
name,
inner_link_tag,
follow_link,
"""


async def fetch(url: str, session: aiohttp.ClientSession) -> str:
    random_user_agent = {"User-Agent": random.choice(USER_AGENTS)}
    async with session.get(url, headers=random_user_agent) as response:
        response_text = response.text
        logging.debug(
            f"random_header: {random_user_agent}\nresponse_text: {response_text}"
        )
        return await response.text()

async def async_bs4_crawler(session: aiohttp.ClientSession, url_obj, test: bool = False) -> dict[str, list[str]]:
    rows = {
        "title": [],
        "link": [],
        "description": [],
        "pubdate": [],
        "location": [],
        "timestamp": [],
    }

    name = url_obj["name"]
    url_prefix = url_obj["url"]
    elements_path = url_obj["elements_path"][0]
    pages = url_obj["pages_to_crawl"]
    start_point = url_obj["start_point"]
    strategy = url_obj["strategy"]
    follow_link = url_obj["follow_link"]
    inner_link_tag = url_obj["inner_link_tag"]

    logging.info(f"{name} has started")
    logging.debug(f"All parameters for {name}:\n{url_obj}")

    async with aiohttp.ClientSession() as session:
        for i in range(start_point, pages + 1):
            url = url_prefix + str(i)

            try:
                html = await fetch(url, session)
                soup = bs4.BeautifulSoup(html, "lxml")
                logging.info(f"Crawling {url} with {strategy} strategy")

                if strategy == "main":
                    try:
                        new_rows = await async_main_strategy_bs4(
                            CUR,
                            session,
                            elements_path,
                            name,
                            inner_link_tag,
                            follow_link,
                            soup,
                            test
                        )
                    except Exception as e:
                        error_message = f"{type(e).__name__} in **async_main_strategy_bs4()** while crawling {url}.\n\n{e}"
                        logging.error(f"{error_message}\n", exc_info=True)
                        continue

                elif strategy == "container":
                    try:
                        new_rows = await async_container_strategy_bs4(
                            CUR,
                            session,
                            elements_path,
                            name,
                            inner_link_tag,
                            follow_link,
                            soup,
                            test
                        )
                    except Exception as e:
                        error_message = f"{type(e).__name__} in **async_container_strategy_bs4()** while crawling {url}.\n\n{e}"
                        logging.error(f"{error_message}\n", exc_info=True)
                        continue
                elif strategy == "occ":
                    try:
                        new_rows = await async_occ_mundial(
                            CUR,
                            session,
                            elements_path,
                            name,
                            inner_link_tag,
                            follow_link,
                            soup,
                            test
                        )
                    except Exception as e:
                        error_message = f"{type(e).__name__} in **async_occ_mundial()** while crawling {url}.\n\n{e}"
                        logging.error(f"{error_message}\n", exc_info=True)
                        continue
                for key in rows:
                    rows[key].extend(new_rows.get(key, []))

            except Exception as e:
                error_message = f"{type(e).__name__} occured before deploying crawling strategy on {url}.\n\n{e}"
                logging.error(f"{error_message}\n", exc_info=True)
                continue
    return rows

async def gather_json_loads_bs4(session: aiohttp.ClientSession, json_data_path: str, test: bool = False) -> None:
    with open(json_data_path) as f:
        data = json.load(f)
        urls = data[0]["urls"]

    tasks = [async_bs4_crawler(session, url_obj, test) for url_obj in urls]
    results = await asyncio.gather(*tasks)

    combined_data = {
        "title": [],
        "link": [],
        "description": [],
        "pubdate": [],
        "location": [],
        "timestamp": [],
    }
    for result in results:
        for key in combined_data:
            combined_data[key].extend(result[key])

    title_len = len(combined_data["title"])
    link_len = len(combined_data["link"])
    description_len = len(combined_data["description"])
    pubdate_len = len(combined_data["pubdate"])
    location_len = len(combined_data["location"])
    timestamp_len = len(combined_data["timestamp"])

    lengths_info = """
        Titles: {}
        Links: {}
        Descriptions: {}
        Pubdates: {}
        Locations: {}
        Timestamps: {}
        """.format(
        title_len,
        link_len,
        description_len,
        pubdate_len,
        location_len,
        timestamp_len,
    )

    if (
        title_len
        == link_len
        == description_len
        == pubdate_len
        == location_len
        == timestamp_len
    ):
        clean_postgre_bs4(
            df=pd.DataFrame(combined_data),
            function_postgre=crawled_df_to_db,
        )
    else:
        logging.error(
            f"ERROR ON ASYNC BS4. LISTS DO NOT HAVE SAME LENGHT. FIX {lengths_info}"
        )
        pass

async def async_bs4_template(json_data_path: str = JSON, test: bool = False):
    start_time = asyncio.get_event_loop().time()


    async with aiohttp.ClientSession() as session:
        await gather_json_loads_bs4(session, json_data_path, test)

    elapsed_time = asyncio.get_event_loop().time() - start_time
    logging.info(f"Async BS4 crawlers finished! all in: {elapsed_time:.2f} seconds.")

    CONN.commit()
    CUR.close()
    CONN.close()


async def main():
    await async_bs4_template()

