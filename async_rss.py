#!/usr/local/bin/python3

import feedparser
import timeit
import os
import json
import pretty_errors
import sys
import bs4
import requests
from datetime import date, datetime
from dotenv import load_dotenv
from utils.rss_utils import clean_postgre_rss
from utils.handy import *
from utils.FollowLink import async_follow_link
import asyncio
import aiohttp

""" LOAD THE ENVIRONMENT VARIABLES """

load_dotenv()

PROD = os.environ.get('JSON_PROD_RSS_READER')
TEST = os.environ.get('JSON_TEST_RSS_READER')
SAVE_PATH = os.environ.get('SAVE_PATH_RSS_READER')
user = os.environ.get('user')
password = os.environ.get('password')
host = os.environ.get('host')
port = os.environ.get('port')
database = os.environ.get('database')

# Create a connection to the database & cursor
conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
cur = conn.cursor()

async def async_rss_template(pipeline):
	start_time = asyncio.get_event_loop().time()
	
	LoggingMasterCrawler()

	#DETERMINING WHICH JSON TO LOAD & WHICH POSTGRE TABLE WILL BE USED

	JSON = None
	POSTGRESQL = None

	if PROD and TEST:
		JSON, POSTGRESQL = test_or_prod(pipeline, PROD, TEST, to_postgre, test_postgre)

	# Check that JSON and POSTGRESQL have been assigned valid values
	if JSON is None or POSTGRESQL is None:
		logging.error("Error: JSON and POSTGRESQL must be assigned valid values.")
		return

	#print("\n", f"Reading {file}... ", "\n")
	print("\n", "Crawler launched on RSS Feeds.")
	logging.info("Async RSS crawler deployed!")

	async def async_rss_reader(session, url_obj):
		total_pubdates = []
		total_titles = []
		total_links = []
		total_locations = []
		total_descriptions = []
		total_timestamps=[]
		rows = {"title": total_titles,
				"link": total_links,
				"description": total_descriptions,
				"pubdate": total_pubdates,
				"location": total_locations,
				"timestamp": total_timestamps}

		""" LOAD THE VARIABLES """

		url = url_obj['url']
		loc_tag = url_obj['location_tag']
		follow_link = url_obj['follow_link']
		inner_link_tag = url_obj['inner_link_tag']
		#ADD MORE VARIABLES IF REQUIRED
		
		try:
			async with aiohttp.ClientSession() as session:
				async with session.get(url) as response:
					if response.status == 200:
						feed_data = await response.text()
						feed = feedparser.parse(feed_data)
						for entry in feed.entries:
							# create a new dictionary to store the data for the current job
							job_data = {}

							job_data["title"] = entry.title if 'title' in entry else "NaN"
							#print(job_data["title"])

							job_data["link"] = entry.link if 'link' in entry else "NaN"
							
							if await link_exists_in_db(link=job_data["link"], cur=cur):
								#logging.info(f"""Link {job_data["link"]} already found in the db. Skipping... """)
								continue
							else:
								default = entry.description if 'description' in entry else "NaN"
								if follow_link == 'yes':
									job_data["description"] = ""
									job_data["description"] = await async_follow_link(session=session, followed_link=job_data['link'], description_final=job_data["description"], inner_link_tag=inner_link_tag, default=default) # type: ignore
								else:
									job_data["description"] = default
								
								today = date.today()
								job_data["pubdate"] = today

								job_data["location"] = getattr(entry, loc_tag) if hasattr(entry, loc_tag) else "NaN"

								#job_data["description"]= entry.description if 'description' in entry else "NaN"

								timestamp = datetime.now()
								job_data["timestamp"] = timestamp
								
								# add the data for the current job to the rows list
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
		except aiohttp.ClientError as e:
			print(f"An error occurred: {e}. Skipping URL {url}")
			logging.error(f"An error occurred: {e}. Skipping URL {url}")
			pass
		return rows
	
	async def gather_json_loads_rss(session):
		with open(JSON) as f:
			data = json.load(f)
			urls = data[0]["urls"]

		tasks = [async_rss_reader(session, url_obj) for url_obj in urls]
		results = await asyncio.gather(*tasks)

		# Combine the results
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
				timestamp_len
			)
		if title_len == link_len == description_len == pubdate_len == location_len == timestamp_len:
			logging.info("async_rss: LISTS HAVE THE SAME LENGHT. SENDING TO POSTGRE")
			clean_postgre_rss(df=pd.DataFrame(combined_data), csv_path=SAVE_PATH, db=POSTGRESQL)
		else:
			logging.error(f"ERROR ON async_rss. LISTS DO NOT HAVE SAME LENGHT. FIX: \n {lengths_info}")
			pass

	async with aiohttp.ClientSession() as session:
		await gather_json_loads_rss(session)

	elapsed_time = asyncio.get_event_loop().time() - start_time
	print(f"Async RSS readers finished! all in: {elapsed_time:.2f} seconds.", "\n")
	logging.info(f"Async RSS readers finished! all in: {elapsed_time:.2f} seconds.")

async def main():
	await async_rss_template("TEST")

if __name__ == "__main__":
	asyncio.run(main())