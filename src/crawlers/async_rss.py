#!/usr/local/bin/python3

from collections.abc import Callable, Coroutine
from typing import Any
import feedparser
import logging
from psycopg2.extensions import cursor
from models import ApiConfig
import aiohttp
import pandas as pd
from datetime import date, datetime
from src.utils.handy import link_exists_in_db
from src.utils.FollowLink import async_follow_link
from feedparser import FeedParserDict

# Set up named logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

async def __async_get_feed_entries(feed: FeedParserDict,
	cur: cursor,
	session: aiohttp.ClientSession,
	rss_config: Any,
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

	for entry in feed.entries:

		title = getattr(entry, rss_config.title_tag) if hasattr(entry, rss_config.location_tag) else "NaN"

		link = getattr(entry, rss_config.link_tag) if hasattr(entry, rss_config.location_tag) else "NaN"
		
		if await link_exists_in_db(link=link, cur=cur, test=test):
			logging.debug(
				f"Link {rss_config.link_tag} already found in the db. Skipping..."
			)
			continue
		
		default = getattr(entry, rss_config.description_tag) if hasattr(entry, rss_config.location_tag) else "NaN"
		if rss_config.follow_link == 'yes':
			job_data["description"] = ""
			job_data["description"] = await async_follow_link(session=session, followed_link=job_data['link'], description_final=job_data["description"], inner_link_tag=inner_link_tag, default=default) # type: ignore
		else:
			job_data["description"] = default
		
		today = date.today()
		job_data["pubdate"] = today

		job_data["location"] = getattr(entry, rss_config.location_tag) if hasattr(entry, rss_config.location_tag) else "NaN"

		timestamp = datetime.now()
		job_data["timestamp"] = timestamp
		

def clean_postgre_rss(df: pd.DataFrame) -> pd.DataFrame:
	df = df.drop_duplicates()
	#Cleaning columns
	for col in df.columns:
		if col == 'description':
			if not df[col].empty:  # Check if the column has any rows
				df[col] = df[col].astype(str)  # Convert the entire column to string
				df[col] = df[col].str.replace(r'<.*?>|[{}[\]\'",]', '', regex=True) #Remove html tags & other characters
		elif col == 'location':
			if not df[col].empty:  # Check if the column has any rows
				df[col] = df[col].astype(str)  # Convert the entire column to string
				df[col] = df[col].str.replace(r'<.*?>|[{}[\]\'",]', '', regex=True) #Remove html tags & other characters
				#df[col] = df[col].str.replace(r'[{}[\]\'",]', '', regex=True)
				df[col] = df[col].str.replace(r'\b(\w+)\s+\1\b', r'\1', regex=True) # Removes repeated words
				df[col] = df[col].str.replace(r'\d{4}-\d{2}-\d{2}', '', regex=True)  # Remove dates in the format "YYYY-MM-DD"
				df[col] = df[col].str.replace(r'(USD|GBP)\d+-\d+/yr', '', regex=True)  # Remove USD\d+-\d+/yr or GBP\d+-\d+/yr.
				df[col] = df[col].str.replace('[-/]', ' ', regex=True)  # Remove -
				df[col] = df[col].str.replace(r'(?<=[a-z])(?=[A-Z])', ' ', regex=True)  # Insert space between lowercase and uppercase letters
				pattern = r'(?i)\bRemote Job\b|\bRemote Work\b|\bRemote Office\b|\bRemote Global\b|\bRemote with frequent travel\b'     # Define a regex patter for all outliers that use remote 
				df[col] = df[col].str.replace(pattern, 'Worldwide', regex=True)
				df[col] = df[col].replace('(?i)^remote$', 'Worldwide', regex=True) # Replace 
				df[col] = df[col].str.strip()  # Remove trailing white space

	#Log it 
	logging.info('Finished RSS Reader. Results below ⬇︎')
	
	return df


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

        new_rows = await __async_get_feed_entries(cur, jobs, session, api_config, test)
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
