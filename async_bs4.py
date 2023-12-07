#!/usr/local/bin/python3

import bs4
import pandas as pd
import re
import pretty_errors
from urllib.error import HTTPError
import timeit
import traceback
from selenium import webdriver
from selenium.webdriver.common.by import By
from dateutil.parser import parse
from datetime import date, datetime
import json
import logging
import requests
import os
from utils.FollowLink import async_follow_link
from dotenv import load_dotenv
from utils.handy import *
from utils.bs4_utils import *
import asyncio
import aiohttp
""" LOAD THE ENVIRONMENT VARIABLES """

#TODO: Move Himalayas to API

load_dotenv()

PROD = os.environ.get('JSON_PROD_BS4')
TEST = os.environ.get('JSON_TEST_BS4')
SAVE_PATH = os.environ.get('SAVE_PATH_BS4')
user = os.environ.get('user')
password = os.environ.get('password')
host = os.environ.get('host')
port = os.environ.get('port')
database = os.environ.get('database')

# Create a connection to the database & cursor
conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
cur = conn.cursor()

async def async_bs4_template(pipeline):

	#start timer
	start_time = asyncio.get_event_loop().time()

	""" DETERMINING WHICH JSON TO LOAD & WHICH POSTGRE TABLE WILL BE USED """

	LoggingMasterCrawler()

	#DETERMINING WHICH JSON TO LOAD & WHICH POSTGRE TABLE WILL BE USED

	JSON = None
	POSTGRESQL = None

	if PROD and TEST:
		JSON, POSTGRESQL = test_or_prod(pipeline, PROD, TEST, to_postgre, test_postgre)

	if JSON is None or POSTGRESQL is None:
		logging.error("Error: JSON and POSTGRESQL must be assigned valid values.")
		return

	logging.info("Async BS4 crawler deployed!.")

	async def fetch(url, session):
		async with session.get(url) as response:
			return await response.text()

	async def async_bs4_crawler(session, url_obj):

		total_links = []
		total_titles = []
		total_pubdates = []
		total_locations = []
		total_descriptions = []
		total_timestamps = []
		rows = {"title": total_titles,
				"link": total_links,
				"description": total_descriptions,
				"pubdate": total_pubdates,
				"location": total_locations,
				"timestamp": total_timestamps}

		#Extract the name of the crawler
		name = url_obj['name']
		print("\n", f"{name} has started", "\n")
		# Extract the 'url' key from the current dictionary and assign it to the variable 'url_prefix'
		url_prefix = url_obj['url']
		# Extract the first dictionary from the 'elements_path' list in the current dictionary and assign it to the variable 'elements_path'
		elements_path = url_obj['elements_path'][0]
			#Each site is different to a json file can give us the flexibility we need
		pages = url_obj['pages_to_crawl']
		#Extract the number in which the range is going to start from
		start_point = url_obj['start_point']
		#strategy
		strategy = url_obj['strategy']
		#Whether to follow link
		follow_link = url_obj['follow_link']
		#Extract inner link if follow link
		inner_link_tag = url_obj['inner_link_tag']
		# You need to +1 because range is exclusive
		async with aiohttp.ClientSession() as session:
				# ... (same URL processing logic as before)

				for i in range(start_point, pages + 1):
					url = url_prefix + str(i)

					try:
						html = await fetch(url, session) # type: ignore
						soup = bs4.BeautifulSoup(html, 'lxml')
						print(f"Crawling {url} with {strategy} strategy")
						if strategy == "main":
							jobs = soup.select(elements_path["jobs_path"])
							if jobs:
								try:
									for job in jobs:

										# create a new dictionary to store the data for the current job
										job_data = {}

										title_element = job.select_one(elements_path["title_path"])
										job_data["title"] = title_element.text if title_element else "NaN"

										link_element = job.select_one(elements_path["link_path"])
										job_data["link"] = name + link_element["href"] if link_element else "NaN"

										""" WHETHER THE LINK IS IN THE DB """
										if await link_exists_in_db(link=job_data["link"], cur=cur):
											#logging.info(f"""Link {job_data["link"]} already found in the db. Skipping... """)
											continue
										else:
											""" WHETHER TO FOLLOW LINK """
											description_default = job.select_one(elements_path["description_path"])
											default = description_default.text if description_default else "NaN"
											if follow_link == "yes":
												job_data["description"] = ""
												job_data["description"] = await async_follow_link(session=session, followed_link=job_data['link'], description_final=job_data["description"], inner_link_tag=inner_link_tag, default=default) # type: ignore
											else:
												# Get the descriptions & append it to its list
												job_data["description"]= default

											#PUBDATE
											today = date.today()
											job_data["pubdate"] = today

											location_element = job.select_one(elements_path["location_path"])
											job_data["location"] = location_element.text if location_element else "NaN"

											timestamp = datetime.now() # type: ignore
											job_data["timestamp"] = timestamp

													# add the data for the current job to the rows list
											total_links.append(job_data["link"])
											total_titles.append(job_data["title"])
											total_pubdates.append(job_data["pubdate"])
											total_locations.append(job_data["location"])
											total_timestamps.append(job_data["timestamp"])
											total_descriptions.append(job_data["description"])
								except (Exception) as e:
									error_message = f"{type(e).__name__} **while** getting the elements in {url}. {traceback.format_exc()}"
									print(error_message)
									logging.error(f"{error_message}\n")
									continue
						else:
							# Identify the container with all the jobs
							container = soup.select_one(elements_path["jobs_path"])

							try:
								if container:
									# Identify the elements for each job
									job_elements = list(zip(
										container.select(elements_path["title_path"]),
										container.select(elements_path["link_path"]),
										container.select(elements_path["description_path"]),
										container.select(elements_path["location_path"]),
									))

									for title_element, link_element, description_element, location_element in job_elements:
										# Process the elements for the current job
										title = title_element.get_text(strip=True) if title_element else "NaN"
										link = name + link_element.get("href") if link_element else "NaN"
										description_default = description_element.get_text(strip=True) if description_element else "NaN"
										location = location_element.get_text(strip=True) if location_element else "NaN"

										# Check if the link exists in the database
										if await link_exists_in_db(link=link, cur=cur):
											continue

										# Follow the link if specified
										description = ''
										if follow_link == "yes":
											description = await async_follow_link(session, link, description, inner_link_tag, description_default)

										# Add the data for the current job to the lists
										total_titles.append(title)
										total_links.append(link)
										total_descriptions.append(description)
										total_locations.append(location)
										total_pubdates.append(date.today())
										total_timestamps.append(datetime.now())
							except:
								print(f"An error occurred: CONTAINER NOT FOUND.. Skipping URL {url}")
								logging.error(f"An error occurred: CONTAINER NOT FOUND.. Skipping URL {url}")
								continue
					except aiohttp.ClientError as e:
						print(f"An error occurred: {e}. Skipping URL {url}")
						logging.error(f"An error occurred: {e}. Skipping URL {url}")
						continue
		return rows

	async def gather_json_loads_bs4(session):
		with open(JSON) as f:
			data = json.load(f)
			urls = data[0]["urls"]

		tasks = [async_bs4_crawler(session, url_obj) for url_obj in urls]
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
			logging.info("BS4: LISTS HAVE THE SAME LENGHT. SENDING TO POSTGRE")
			clean_postgre_bs4(df=pd.DataFrame(combined_data), S=SAVE_PATH, Q=POSTGRESQL)
		else:
			logging.error(f"ERROR ON ASYNC BS4. LISTS DO NOT HAVE SAME LENGHT. FIX {lengths_info}")
			pass
	
	async with aiohttp.ClientSession() as session:
		await gather_json_loads_bs4(session)

	elapsed_time = asyncio.get_event_loop().time() - start_time
	print(f"Async BS4 crawlers finished! all in: {elapsed_time:.2f} seconds.", "\n")
	logging.info(f"Async BS4 crawlers finished! all in: {elapsed_time:.2f} seconds.")

async def main():
	await async_bs4_template("TEST")

if __name__ == "__main__":
	asyncio.run(main())