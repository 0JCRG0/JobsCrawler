#!/usr/local/bin/python3

from collections.abc import Callable, Coroutine
from typing import Any
import feedparser
from utils.logger_helper import get_custom_logger
from psycopg2.extensions import cursor
import aiohttp
import pandas as pd
from datetime import date, datetime
from utils.handy import link_exists_in_db
from utils.FollowLink import async_follow_link
from feedparser import FeedParserDict

# Set up named logger
logger = get_custom_logger(__name__)
 

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
		
		if await link_exists_in_db(
			link=rss_config.link_tag, cur=cur, test=test
		):
			logger.debug(
				f"Link {rss_config.link_tag} already found in the db. Skipping..."
			)
			continue
		
		default = getattr(entry, rss_config.description_tag) if hasattr(entry, rss_config.location_tag) else "NaN"
		description = ""
		if rss_config.follow_link == 'yes':
			description = await async_follow_link(
					session=session,
					followed_link=link,
					description_final=description,
					inner_link_tag=rss_config.inner_link_tag,
					default=default,
				)
		else:
			description = default
		
		today = date.today()

		location = getattr(entry, rss_config.location_tag) if hasattr(entry, rss_config.location_tag) else "NaN"

		timestamp = datetime.now()
		for key, value in zip(
			total_jobs_data.keys(),
			[title, link, description, today, location, timestamp],
		):
			total_jobs_data[key].append(value)

	return total_jobs_data
		

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
	logger.info('Finished RSS Reader. Results below ⬇︎')
	
	return df


async def async_rss_reader(
	fetch_func: Callable[[aiohttp.ClientSession], Coroutine[Any, Any, str]],
	session: aiohttp.ClientSession,
	rss_config: Any,
	cur: cursor,
	test: bool = False,
) -> dict[str, list[str]]:
	rows = {
		key: []
		for key in ["title", "link", "description", "pubdate", "location", "timestamp"]
	}

	logger.info(f"{rss_config.url} has started")
	logger.debug(f"All parameters for {rss_config.url}:\n{rss_config}")

	try:
		response = await fetch_func(session)
		logger.debug(f"Successful request on {rss_config.url}")
		feed = feedparser.parse(response)

		new_rows = await __async_get_feed_entries(feed, cur, session, rss_config, test)
		if new_rows:
			for key in rows:
				rows[key].extend(new_rows.get(key, []))
	except Exception as e:
		logger.error(
			f"{type(e).__name__} occurred before deploying crawling strategy on {rss_config.url}.\n\n{e}",
			exc_info=True,
		)
		pass
	return rows
