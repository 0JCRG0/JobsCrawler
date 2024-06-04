from typing import Callable
from psycopg2.extensions import cursor
import bs4
import pandas as pd
import pretty_errors  # noqa: F401
from datetime import date, datetime
import logging
from utils.FollowLink import async_follow_link, async_follow_link_title_description
from utils.handy import link_exists_in_db
import aiohttp


async def async_main_strategy_bs4(
    cur: cursor,
    session: aiohttp.ClientSession,
    elements_path: dict,
    name: str,
    inner_link_tag: str,
    follow_link: str,
    soup: bs4.BeautifulSoup,
    test: bool = False
):
    total_links = []
    total_titles = []
    total_pubdates = []
    total_locations = []
    total_descriptions = []
    total_timestamps = []

    jobs = soup.select(elements_path["jobs_path"])
    assert (
        jobs is not None
    ), "No elements found for 'jobs' in async_main_strategy_bs4(). Check 'elements_path[\"jobs_path\"]'"
    for job in jobs:
        job_data = {}

        title_element = job.select_one(elements_path["title_path"])
        assert (
            title_element is not None
        ), "No elements found for 'title_element' in main_strategy_bs4(). Check 'elements_path[\"title_path\"]'"
        job_data["title"] = title_element.text if title_element else "NaN"

        link_element = job.select_one(elements_path["link_path"])
        assert (
            link_element is not None
        ), "No elements found for 'link_element' in main_strategy_bs4(). Check 'elements_path[\"link_path\"]'"
        job_data["link"] = name + str(link_element["href"]) if link_element else "NaN"

        """ WHETHER THE LINK IS IN THE DB """
        if await link_exists_in_db(link=job_data["link"], cur=cur, test=test):
            logging.debug(
                f"""Link {job_data["link"]} already found in the db. Skipping... """
            )
            continue
        else:
            """WHETHER TO FOLLOW LINK"""
            description_default = job.select_one(elements_path["description_path"])
            default = description_default.text if description_default else "NaN"
            if follow_link == "yes":
                job_data["description"] = ""
                job_data["description"] = await async_follow_link(
                    session=session,
                    followed_link=job_data["link"],
                    description_final=job_data["description"],
                    inner_link_tag=inner_link_tag,
                    default=default,
                )
            else:
                job_data["description"] = default

            today = date.today()
            job_data["pubdate"] = today

            location_element = job.select_one(elements_path["location_path"])
            job_data["location"] = location_element.text if location_element else "NaN"

            timestamp = datetime.now()
            job_data["timestamp"] = timestamp

            total_links.append(job_data["link"])
            total_titles.append(job_data["title"])
            total_pubdates.append(job_data["pubdate"])
            total_locations.append(job_data["location"])
            total_timestamps.append(job_data["timestamp"])
            total_descriptions.append(job_data["description"])

    return {
        "title": total_titles,
        "link": total_links,
        "description": total_descriptions,
        "pubdate": total_pubdates,
        "location": total_locations,
        "timestamp": total_timestamps,
    }


async def async_container_strategy_bs4(
    cur: cursor,
    session: aiohttp.ClientSession,
    elements_path: dict,
    name: str,
    inner_link_tag: str,
    follow_link: str,
    soup: bs4.BeautifulSoup,
    test: bool = False
):
    total_links = []
    total_titles = []
    total_pubdates = []
    total_locations = []
    total_descriptions = []
    total_timestamps = []

    # Identify the container with all the jobs
    container = soup.select_one(elements_path["jobs_path"])
    assert (
        container is not None
    ), "No elements found for 'container' in async_container_strategy_bs4(). Check 'elements_path[\"jobs_path\"]'"

    titles = container.select(elements_path["title_path"])
    assert (
        titles is not None
    ), "No elements found for 'titles' in async_container_strategy_bs4(). Check 'elements_path[\"title_path\"]'"

    links = container.select(elements_path["link_path"])
    assert (
        links is not None
    ), "No elements found for 'links' in async_container_strategy_bs4(). Check 'elements_path[\"link_path\"]'"

    descriptions = container.select(elements_path["description_path"])
    assert (
        descriptions is not None
    ), "No elements found for 'descriptions' in async_container_strategy_bs4(). Check 'elements_path[\"description_path\"]'"

    locations = container.select(elements_path["location_path"])
    assert (
        locations is not None
    ), "No elements found for 'locations' in async_container_strategy_bs4(). Check 'elements_path[\"location_path\"]'"

    # Identify the elements for each job
    job_elements = list(
        zip(
            titles,
            links,
            descriptions,
            locations,
        )
    )

    assert all(
        len(element) == len(job_elements[0]) for element in job_elements
    ), "Not all elements have the same length in async_container_strategy_bs4()"

    for (
        title_element,
        link_element,
        description_element,
        location_element,
    ) in job_elements:
        title = title_element.get_text(strip=True) if title_element else "NaN"
        link = name + str(link_element.get("href")) if link_element else "NaN"
        description_default = (
            description_element.get_text(strip=True) if description_element else "NaN"
        )
        location = location_element.get_text(strip=True) if location_element else "NaN"

        # Check if the link exists in the database
        if await link_exists_in_db(link=link, cur=cur, test=test):
            logging.info(f"""Link {link} already found in the db. Skipping... """)
            continue

        # Follow the link if specified
        description = ""
        if follow_link == "yes":
            description = await async_follow_link(
                session, link, description, inner_link_tag, description_default
            )

        # add the data for the current job to the rows list
        total_titles.append(title)
        total_links.append(link)
        total_descriptions.append(description)
        total_locations.append(location)
        total_pubdates.append(date.today())
        total_timestamps.append(datetime.now())

    return {
        "title": total_titles,
        "link": total_links,
        "description": total_descriptions,
        "pubdate": total_pubdates,
        "location": total_locations,
        "timestamp": total_timestamps,
    }


def clean_postgre_bs4(df: pd.DataFrame, function_postgre: Callable):
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

    function_postgre(df)


async def async_occ_mundial(
    cur: cursor,
    session: aiohttp.ClientSession,
    elements_path: dict,
    name: str,
    inner_link_tag: str,
    follow_link: str,
    soup: bs4.BeautifulSoup,
    test: bool = False
):
    total_links = []
    total_titles = []
    total_pubdates = []
    total_locations = []
    total_descriptions = []
    total_timestamps = []

    # Identify the container with all the jobs
    container = soup.select_one(elements_path["jobs_path"])
    assert (
        container is not None
    ), "No elements found for 'container' in async_container_strategy_bs4(). Check 'elements_path[\"jobs_path\"]'"

    # titles = container.select(elements_path["title_path"])
    # assert titles is not None, "No elements found for 'titles' in async_container_strategy_bs4(). Check 'elements_path[\"title_path\"]'"

    links = container.select(elements_path["link_path"])
    assert (
        links is not None
    ), "No elements found for 'links' in async_container_strategy_bs4(). Check 'elements_path[\"link_path\"]'"

    # Identify the elements for each job
    job_elements = list(links)

    print(len(job_elements))

    # assert all(len(element) == len(job_elements[0]) for element in job_elements), "Not all elements have the same length in async_container_strategy_bs4()"

    for link_element in job_elements:
        # Process the elements for the current job
        # title = title_element.get_text(strip=True) if title_element else "NaN"
        link = name + str(link_element.get("href")) if link_element else "NaN"
        # TODO: Regex on link to only iunclude this: https://www.occ.com.mx/empleo/oferta/17987183-mulesoft-developer/
        # link = re.search(r'https://www\.occ\.com\.mx/empleo/oferta/\d+[^/?]+', link).group()

        # Check if the link exists in the database
        if await link_exists_in_db(link=link, cur=cur, test=test):
            logging.info(f"""Link {link} already found in the db. Skipping... """)
            continue

        # Follow the link if specified
        title = ""
        description = ""
        if follow_link == "yes":
            title, description = await async_follow_link_title_description(
                session,
                link,
                description,
                inner_link_tag,
                elements_path["title_path"],
                "NaN",
            )

        # add the data for the current job to the rows list
        total_titles.append(title)
        total_links.append(link)
        total_descriptions.append(description)
        total_locations.append("MX")
        total_pubdates.append(date.today())
        total_timestamps.append(datetime.now())

    return {
        "title": total_titles,
        "link": total_links,
        "description": total_descriptions,
        "pubdate": total_pubdates,
        "location": total_locations,
        "timestamp": total_timestamps,
    }
