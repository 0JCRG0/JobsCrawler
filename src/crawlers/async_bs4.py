#!/usr/local/bin/python3

from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import aiohttp
import bs4
import pandas as pd
from bs4 import BeautifulSoup
from psycopg2.extensions import cursor

from src.utils.FollowLink import async_follow_link
from src.utils.handy import link_exists_in_db
from src.utils.logger_helper import get_custom_logger

logger = get_custom_logger(__name__)

@dataclass
class Bs4ElementPath:
    """
    Dataclass for storing CSS selectors used to extract job information from HTML.
    
    Attributes:
        jobs_path: CSS selector for the job listing container
        title_path: CSS selector for job title elements
        link_path: CSS selector for job link elements
        location_path: CSS selector for job location elements
        description_path: CSS selector for job description elements
    """
    jobs_path: str
    title_path: str
    link_path: str
    location_path: str
    description_path: str


def clean_postgre_bs4(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and normalize job data before inserting into PostgreSQL database.
    
    Performs various text cleaning operations to standardize data format:
    - Removes duplicates
    - Strips HTML tags
    - Normalizes location data
    - Removes special characters
    - Standardizes remote work descriptions
    
    Args:
        df: DataFrame containing job listing data
        
    Returns:
        Cleaned DataFrame ready for database insertion
    """
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

    logger.info("Finished bs4 crawlers. Results below ⬇︎")

    return df

async def __async_main_strategy_bs4(
    cur: cursor,
    session: aiohttp.ClientSession,
    bs4_config: Any,
    soup: bs4.BeautifulSoup,
    test: bool = False,
):
    """
    Main strategy for extracting job listings from HTML.
    
    Selects individual job elements and extracts data from each one.
    Best for pages where each job listing is a self-contained HTML element.
    
    Args:
        cur: Database cursor for checking existing links
        session: HTTP session for making requests
        bs4_config: Configuration object with crawling parameters
        soup: BeautifulSoup object containing parsed HTML
        test: Whether running in test mode
        
    Returns:
        Dictionary containing extracted job data
    
    Raises:
        ValueError: If required elements are not found in the HTML
    """
    total_jobs_data = {
        "title": [],
        "link": [],
        "description": [],
        "pubdate": [],
        "location": [],
        "timestamp": [],
    }

    bs4_element_path = Bs4ElementPath(**bs4_config.elements_path)

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

        link = bs4_config.name + str(link_element["href"])

        if await link_exists_in_db(link=link, cur=cur, test=test):
            logger.debug(f"Link {link} already found in the db. Skipping...")
            continue

        description_element = job.select_one(bs4_element_path.description_path)
        description = description_element.text if description_element else "NaN"
        if bs4_config.follow_link == "yes":
            description = await async_follow_link(
                session=session,
                followed_link=link,
                description_final="",
                inner_link_tag=bs4_config.inner_link_tag,
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
    return total_jobs_data


async def __async_container_strategy_bs4(
    cur: cursor,
    session: aiohttp.ClientSession,
    bs4_config: Any,
    soup: bs4.BeautifulSoup,
    test: bool = False,
):
    """
    Container strategy for extracting job listings from HTML.
    
    Selects a single container element and then extracts collections of titles,
    links, descriptions, and locations, then pairs them together.
    Best for pages where job attributes are organized in parallel collections.
    
    Args:
        cur: Database cursor for checking existing links
        session: HTTP session for making requests
        bs4_config: Configuration object with crawling parameters
        soup: BeautifulSoup object containing parsed HTML
        test: Whether running in test mode
        
    Returns:
        Dictionary containing extracted job data
    
    Raises:
        ValueError: If required elements are not found in the HTML
    """
    bs4_element_path = Bs4ElementPath(**bs4_config.elements_path)

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
        link = bs4_config.name + (link_element.get("href") or "NaN")
        description_default = description_element.get_text(strip=True) or "NaN"
        location = location_element.get_text(strip=True) or "NaN"
        if await link_exists_in_db(link=link, cur=cur, test=test):
            logger.debug(f"Link {link} already found in the db. Skipping...")
            continue

        description = (
            await async_follow_link(
                session, link, description_default, bs4_config.inner_link_tag
            )
            if bs4_config.follow_link == "yes"
            else description_default
        )

        now = datetime.now()
        total_data["title"].append(title)
        total_data["link"].append(link)
        total_data["description"].append(description)
        total_data["pubdate"].append(date.today())
        total_data["location"].append(location)
        total_data["timestamp"].append(now)
    
    return total_data


async def _crawling_strategy(
    session: aiohttp.ClientSession,
    bs4_config: Any,
    soup: BeautifulSoup,
    test: bool,
    cur: cursor,
) -> dict[str, list[str]] | None:
    """
    Selects and executes the appropriate crawling strategy based on configuration.
    
    Acts as a dispatcher that routes to the correct strategy implementation
    and handles any errors that occur during crawling.
    
    Args:
        session: HTTP session for making requests
        bs4_config: Configuration object with crawling parameters and strategy
        soup: BeautifulSoup object containing parsed HTML
        test: Whether running in test mode
        cur: Database cursor for checking existing links
        
    Returns:
        Dictionary containing extracted job data or None if an error occurred
        
    Raises:
        ValueError: If an unrecognized strategy is specified
    """
    strategy_map = {
        "main": __async_main_strategy_bs4,
        "container": __async_container_strategy_bs4,
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
    """
    Main entry point for asynchronous BeautifulSoup-based web crawling.
    
    Crawls multiple pages of a job listing website according to the provided
    configuration, extracting job details using the specified strategy.
    
    Args:
        fetch_func: Function that fetches HTML content using the provided session
        session: HTTP session for making requests
        bs4_config: Configuration object with crawling parameters
        cur: Database cursor for checking existing links
        test: Whether running in test mode
        
    Returns:
        Dictionary containing all extracted job data from all crawled pages
    """
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
                    rows[key].extend(new_rows.get(key, []))

        except Exception as e:
            logger.error(
                f"{type(e).__name__} occurred before deploying crawling strategy on {url}.\n\n{e}",
                exc_info=True,
            )
            continue
    return rows
