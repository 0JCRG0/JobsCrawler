from psycopg2.extensions import cursor
import bs4
import pandas as pd
from datetime import date, datetime
import logging
from src.utils.FollowLink import async_follow_link, async_follow_link_title_description
from src.utils.handy import link_exists_in_db
import aiohttp
from types import Bs4Config


async def async_main_strategy_bs4(
    cur: cursor,
    session: aiohttp.ClientSession,
    bs4_element: 'Bs4Config',
    soup: bs4.BeautifulSoup,
    test: bool = False
):
    total_jobs_data = {
        "title": [],
        "link": [],
        "description": [],
        "pubdate": [],
        "location": [],
        "timestamp": [],
    }

    jobs = soup.select(bs4_element.elements_path.jobs_path)
    if not jobs:
        raise ValueError(f"No jobs were found using this selector {bs4_element.elements_path.jobs_path}")

    for job in jobs:
        title_element = job.select_one(bs4_element.elements_path.title_path)
        if not title_element:
            raise ValueError(f"No titles were found using this selector {bs4_element.elements_path.title_path}")

        link_element = job.select_one(bs4_element.elements_path.link_path)
        if not link_element:
            raise ValueError(f"No links were found using this selector {bs4_element.elements_path.link_path}")

        link = bs4_element.name + str(link_element["href"])

        if await link_exists_in_db(link=link, cur=cur, test=test):
            logging.debug(f"Link {link} already found in the db. Skipping...")
            continue

        description_element = job.select_one(bs4_element.elements_path.description_path)
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
        location_element = job.select_one(bs4_element.elements_path.location_path)
        location = location_element.text if location_element else "NaN"

        timestamp = datetime.now()

        for key, value in zip(total_jobs_data.keys(), [title_element.text, link, description, today, location, timestamp]):
            total_jobs_data[key].append(value)

    return total_jobs_data

async def async_container_strategy_bs4(
    cur: cursor,
    session: aiohttp.ClientSession,
    bs4_element: 'Bs4Config',
    soup: bs4.BeautifulSoup,
    test: bool = False
):
    paths = bs4_element.elements_path
    total_data = {
        "title": [],
        "link": [],
        "description": [],
        "pubdate": [],
        "location": [],
        "timestamp": [],
    }

    container = soup.select_one(paths.jobs_path)
    if not container:
        raise ValueError(f"No elements found for 'container'. Check '{paths.jobs_path}'")

    elements = {
        "title": container.select(paths.title_path),
        "link": container.select(paths.link_path),
        "description": container.select(paths.description_path),
        "location": container.select(paths.location_path),
    }

    for key, value in elements.items():
        if not value:
            raise ValueError(f"No elements found for '{key}'. Check 'elements_path[\"{key}_path\"]'")

    job_elements = zip(*elements.values())

    for title_element, link_element, description_element, location_element in job_elements:
        title = title_element.get_text(strip=True) or "NaN"
        link = bs4_element.name + (link_element.get("href") or "NaN")
        description_default = description_element.get_text(strip=True) or "NaN"
        location = location_element.get_text(strip=True) or "NaN"

        if await link_exists_in_db(link=link, cur=cur, test=test):
            logging.debug(f"Link {link} already found in the db. Skipping...")
            continue

        description = await async_follow_link(
            session, link, description_default, bs4_element.inner_link_tag
        ) if bs4_element.follow_link == "yes" else description_default

        now = datetime.now()
        total_data["title"].append(title)
        total_data["link"].append(link)
        total_data["description"].append(description)
        total_data["pubdate"].append(date.today())
        total_data["location"].append(location)
        total_data["timestamp"].append(now)

    return total_data


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


async def async_occ_mundial(
    cur: cursor,
    session: aiohttp.ClientSession,
    element: 'Bs4Config', 
    soup: bs4.BeautifulSoup,
    test: bool = False
):
    # TODO: NOT TESTED.
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
        raise AssertionError("No elements found for 'container'. Check 'elements_path[\"jobs_path\"]'")

    links = container.select(element.elements_path.link_path)
    if not links:
        raise AssertionError("No elements found for 'links'. Check 'elements_path[\"link_path\"]'")

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
        for key, value in zip(["link", "title", "description", "location", "pubdate", "timestamp"],
                            [link, title, description, "MX", today, now]):
            total_data[key].append(value)

    return total_data