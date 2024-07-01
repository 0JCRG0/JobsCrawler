#!/usr/local/bin/python3

import feedparser
from feedparser import FeedParserDict
from datetime import date, datetime
import logging
import psycopg2
from src.utils.handy import link_exists_in_db
from src.utils.FollowLink import async_follow_link
import asyncio
import aiohttp


# Set up named logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class AsyncRssFeedparser:


async def async_rss_template(pipeline):
	start_time = asyncio.get_event_loop().time()


	conn = psycopg2.connect(URL_DB)
	cur = conn.cursor()

	async def async_rss_reader(session, url_obj):
		rows = {
        key: []
        for key in ["title", "link", "description", "pubdate", "location", "timestamp"]
		}
		
		try:
			async with aiohttp.ClientSession() as session:
				async with session.get(url) as response:
					if response.status == 200:
						feed_data = await response.text()
						feed = feedparser.parse(feed_data)
						for entry in feed.entries:
							job_data = {}

							job_data["title"] = getattr(entry, title_tag) if hasattr(entry, loc_tag) else "NaN"

							job_data["link"] = getattr(entry, link_tag) if hasattr(entry, loc_tag) else "NaN"
							
							if await link_exists_in_db(link=job_data["link"], cur=cur, pipeline=pipeline):
								continue
							else:
								default = getattr(entry, description_tag) if hasattr(entry, loc_tag) else "NaN"
								if follow_link == 'yes':
									job_data["description"] = ""
									job_data["description"] = await async_follow_link(session=session, followed_link=job_data['link'], description_final=job_data["description"], inner_link_tag=inner_link_tag, default=default) # type: ignore
								else:
									job_data["description"] = default
								
								today = date.today()
								job_data["pubdate"] = today

								job_data["location"] = getattr(entry, loc_tag) if hasattr(entry, loc_tag) else "NaN"

								timestamp = datetime.now()
								job_data["timestamp"] = timestamp
								
								total_links.append(job_data["link"])
								total_titles.append(job_data["title"])
								total_pubdates.append(job_data["pubdate"])
								total_locations.append(job_data["location"])
								total_timestamps.append(job_data["timestamp"])
								total_descriptions.append(job_data["description"])
					else:
						print(f"""PARSING FAILED ON {url}. Response: {response}. SKIPPING...""", "\n")
						logging.error(f"""PARSING FAILED ON {url}. Response: {response}. SKIPPING...""")
						pass
		except Exception as e:
			logger.error(
				f"{type(e).__name__} occurred before deploying crawling strategy on {url}.\n\n{e}",
				exc_info=True,
			)
			pass
		return rows
	
