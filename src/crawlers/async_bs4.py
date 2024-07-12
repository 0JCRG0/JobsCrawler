#!/usr/local/bin/python3

from collections.abc import Callable, Coroutine
from typing import Any
from dataclasses import dataclass
from bs4 import BeautifulSoup
import logging
from psycopg2.extensions import cursor
import aiohttp
import bs4
import pandas as pd
from datetime import date, datetime
from src.utils.FollowLink import async_follow_link, async_follow_link_title_description
from src.utils.handy import link_exists_in_db

# Set up named logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@dataclass
class Bs4ElementPath():
    jobs_path: str
    title_path: str
    link_path: str
    location_path: str
    description_path: str


def clean_postgre_bs4(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates()

    for col in df.columns:
        if col == "title" or col == "description":
            if not df[col].empty:  # Check if the column has any rows
                df[col] = df[col].astype(str)  # Convert the entire column to string
                df[col] = df[col].str.replace(
                    r'<.*?>|[{}[\]\'",]', "", regex=True
                )  # Remove html tags & other characters
        elif col == "location":
            if not df[col].empty:  # Check if the column has any rows
                df[col] = df[col].astype(str)  # Convert the entire column to string
                df[col] = df[col].str.replace(
                    r'<.*?>|[{}[\]\'",]', "", regex=True
                )  # Remove html tags & other characters
                # df[col] = df[col].str.replace(r'[{}[\]\'",]', '', regex=True)
                df[col] = df[col].str.replace(
                    r"\b(\w+)\s+\1\b", r"\1", regex=True
                )  # Removes repeated words
                df[col] = df[col].str.replace(
                    r"\d{4}-\d{2}-\d{2}", "", regex=True
                )  # Remove dates in the format "YYYY-MM-DD"
                df[col] = df[col].str.replace(
                    r"(USD|GBP)\d+-\d+/yr", "", regex=True
                )  # Remove USD\d+-\d+/yr or GBP\d+-\d+/yr.
                df[col] = df[col].str.replace("[-/]", " ", regex=True)  # Remove -
                df[col] = df[col].str.replace(
                    r"(?<=[a-z])(?=[A-Z])", " ", regex=True
                )  # Insert space between lowercase and uppercase letters
                pattern = r"(?i)\bRemote Job\b|\bRemote Work\b|\bRemote Office\b|\bRemote Global\b|\bRemote with frequent travel\b"  # Define a regex patter for all outliers that use remote
                df[col] = df[col].str.replace(pattern, "Worldwide", regex=True)
                df[col] = df[col].replace(
                    "(?i)^remote$", "Worldwide", regex=True
                )  # Replace
                df[col] = df[col].str.strip()  # Remove trailing white space

    logging.info("Finished bs4 crawlers. Results below ⬇︎")

    return df

async def __async_main_strategy_bs4(
    cur: cursor,
    session: aiohttp.ClientSession,
    bs4_element: Any,
    soup: bs4.BeautifulSoup,
    test: bool = False,
):
    total_jobs_data = {
        "title": [],
        "link": [],
        "description": [],
        "pubdate": [],
        "location": [],
        "timestamp": [],
    }

    bs4_element_path = Bs4ElementPath(**bs4_element.elements_path)

    jobs = soup.select(bs4_element_path.jobs_path)
    if not jobs:
        raise ValueError(
            f"No jobs were found using this selector {bs4_element_path.jobs_path}"
        )

    for job in jobs:
        title_element = job.select_one(bs4_element_path.title_path)
        if not title_element:
            raise ValueError(
                f"No titles were found using this selector {bs4_element_path.title_path}"
            )

        link_element = job.select_one(bs4_element_path.link_path)
        if not link_element:
            raise ValueError(
                f"No links were found using this selector {bs4_element_path.link_path}"
            )

        link = bs4_element.name + str(link_element["href"])

        if await link_exists_in_db(link=link, cur=cur, test=test):
            logging.debug(f"Link {link} already found in the db. Skipping...")
            continue

        description_element = job.select_one(bs4_element_path.description_path)
        description = description_element.text if description_element else "NaN"
        if bs4_element.follow_link == "yes":
            description = await async_follow_link(
                session=session,
                followed_link=link,
                description_final="",
                inner_link_tag=bs4_element.inner_link_tag,
                default=description,
            )

        today = date.today()
        location_element = job.select_one(bs4_element_path.location_path)
        location = location_element.text if location_element else "NaN"

        timestamp = datetime.now()

        for key, value in zip(
            total_jobs_data.keys(),
            [title_element.text, link, description, today, location, timestamp],
        ):
            total_jobs_data[key].append(value)
    logging.info(f"{bs4_element.url}: \n{total_jobs_data} \n\n")
    return total_jobs_data


async def __async_container_strategy_bs4(
    cur: cursor,
    session: aiohttp.ClientSession,
    bs4_element: Any,
    soup: bs4.BeautifulSoup,
    test: bool = False,
):
    bs4_element_path = Bs4ElementPath(**bs4_element.elements_path)

    total_data = {
        "title": [],
        "link": [],
        "description": [],
        "pubdate": [],
        "location": [],
        "timestamp": [],
    }

    container = soup.select_one(bs4_element_path.jobs_path)
    if not container:
        raise ValueError(
            f"No elements found for 'container'. Check '{bs4_element_path.jobs_path}'"
        )

    elements = {
        "title": container.select(bs4_element_path.title_path),
        "link": container.select(bs4_element_path.link_path),
        "description": container.select(bs4_element_path.description_path),
        "location": container.select(bs4_element_path.location_path),
    }

    for key, value in elements.items():
        if not value:
            raise ValueError(
                f"No elements found for '{key}'. Check 'elements_path[\"{key}_path\"]'"
            )

    job_elements = zip(*elements.values())

    for (
        title_element,
        link_element,
        description_element,
        location_element,
    ) in job_elements:
        title = title_element.get_text(strip=True) or "NaN"
        link = bs4_element.name + (link_element.get("href") or "NaN")
        description_default = description_element.get_text(strip=True) or "NaN"
        location = location_element.get_text(strip=True) or "NaN"
        # TODO: IS IT SENDING THE DATA TO THE TEST DB? WHAT IS THE VALUE OF TEST?
        if await link_exists_in_db(link=link, cur=cur, test=test):
            logging.debug(f"Link {link} already found in the db. Skipping...")
            continue

        description = (
            await async_follow_link(
                session, link, description_default, bs4_element.inner_link_tag
            )
            if bs4_element.follow_link == "yes"
            else description_default
        )

        now = datetime.now()
        total_data["title"].append(title)
        total_data["link"].append(link)
        total_data["description"].append(description)
        total_data["pubdate"].append(date.today())
        total_data["location"].append(location)
        total_data["timestamp"].append(now)
    
    #TODO: THE DICTIONARY IS EMPTY. DUNNO IF IT IS BECAUSE THE DATA IS ALREADY IN THE DB OR AN ISSUE WITH THE FUNCTION.
    logging.info(f"{bs4_element.url}: \n{total_data} \n\n")
    return total_data


async def __async_occ_mundial(
    cur: cursor,
    session: aiohttp.ClientSession,
    element: Any,
    soup: bs4.BeautifulSoup,
    test: bool = False,
):
    # NOT TESTED. NOT CURRENTLY USING.
    total_data = {
        "title": [],
        "link": [],
        "description": [],
        "pubdate": [],
        "location": [],
        "timestamp": [],
    }

    container = soup.select_one(element.elements_path.jobs_path)
    if not container:
        raise AssertionError(
            "No elements found for 'container'. Check 'elements_path[\"jobs_path\"]'"
        )

    links = container.select(element.elements_path.link_path)
    if not links:
        raise AssertionError(
            "No elements found for 'links'. Check 'elements_path[\"link_path\"]'"
        )

    print(f"Number of job elements: {len(links)}")

    for link_element in links:
        link = f"{element.name}{link_element.get('href')}" if link_element else "NaN"

        if await link_exists_in_db(link=link, cur=cur, test=test):
            logging.info(f"Link {link} already found in the db. Skipping...")
            continue

        title, description = ("", "")
        if element.follow_link == "yes":
            title, description = await async_follow_link_title_description(
                session,
                link,
                description,
                element.inner_link_tag,
                element.elements_path.title_path,
                "NaN",
            )

        today = date.today()
        now = datetime.now()
        for key, value in zip(
            ["link", "title", "description", "location", "pubdate", "timestamp"],
            [link, title, description, "MX", today, now],
        ):
            total_data[key].append(value)

    return total_data


async def _crawling_strategy(
    session: aiohttp.ClientSession,
    bs4_config: Any,
    soup: BeautifulSoup,
    test: bool,
    cur: cursor,
) -> dict[str, list[str]] | None:
    strategy_map = {
        "main": __async_main_strategy_bs4,
        "container": __async_container_strategy_bs4,
        "occ": __async_occ_mundial,
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
    bs4_config: Any,
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
            new_rows = await _crawling_strategy(session, bs4_config, soup, test, cur)
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
