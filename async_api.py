#!/usr/local/bin/python3

import requests
import json
import pretty_errors
import pandas as pd
import timeit
import os
import logging
import bs4
from datetime import date
from datetime import datetime
from dotenv import load_dotenv
from utils.handy import *
from utils.api_utils import clean_postgre_api
from utils.FollowLink import async_follow_link
import asyncio
import aiohttp


""" LOAD THE ENVIRONMENT VARIABLES """

load_dotenv()

PROD = os.environ.get('JSON_PROD_API')
TEST = os.environ.get('JSON_TEST_API')
SAVE_PATH = os.environ.get('SAVE_PATH_API')
user = os.environ.get('user')
password = os.environ.get('password')
host = os.environ.get('host')
port = os.environ.get('port')
database = os.environ.get('database')

# Create a connection to the database & cursor
conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
cur = conn.cursor()

async def async_api_template(pipeline):
	#Start the timer
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

	print("\n", "ASYNC APIs HAS STARTED.")
	logging.info("Async API crawler deployed!.")

	async def async_api_fetcher(session, api_obj):

		total_titles = []
		total_links = []
		total_descriptions = []
		total_pubdates = []
		total_locations = []
		total_timestamps=[]
		rows = {"title": total_titles,
				"link": total_links,
				"description": total_descriptions,
				"pubdate": total_pubdates,
				"location": total_locations,
				"timestamp": total_timestamps}

		#Extract the name of the site
		name = api_obj['name']
		# Extract the 'api' key from the current dictionary and assign it to the variable 'api
		api = api_obj['api']
		# Extract the first dictionary from the 'elements_path' list in the current dictionary and assign it to the variable 'elements_path'
		elements_path = api_obj['elements_path'][0]
		#Extract the class of the JSON
		class_json = api_obj['class_json']
		#Whether to follow link
		follow_link = api_obj['follow_link']
		#Extract inner link if follow link
		inner_link_tag = api_obj['inner_link_tag']
		headers = {"User-Agent": "my-app"}
		
		async with aiohttp.ClientSession() as session:
			try:
				print("\n", f"Requesting {name}...")
				async with session.get(api, headers=headers) as response:
					if response.status != 200:
						print(f"Received non-200 response ({response.status}) for API: {api}. Skipping...")
						logging.warning(f"Received non-200 response ({response.status}) for API: {api}. Skipping...")
						pass
					else:
						try:
							response_text = await response.text()
							data = json.loads(response_text)
							print(f"Successful request on {api}", "\n")
							jobs = class_json_strategy(data, elements_path, class_json)
							if jobs:
								for job in jobs:
									job_data = {}

									job_data["title"] = job.get(elements_path["title_tag"], "NaN")

									job_data["link"] = job.get(elements_path["link_tag"], "NaN")
									
									""" WHETHER THE LINK IS IN THE DB """
									if await link_exists_in_db(link=job_data["link"], cur=cur):
										#logging.info(f"""Link {job_data["link"]} already found in the db. Skipping... """)
										continue
									else:
										""" WHETHER TO FOLLOW LINK """
										if follow_link == "yes":
											default = job.get(elements_path["description_tag"], "NaN")
											job_data["description"] = ""
											job_data["description"] = await async_follow_link(session=session, followed_link=job_data['link'], description_final=job_data["description"], inner_link_tag=inner_link_tag, default=default)									 # type: ignore
										else:
											job_data["description"] = job.get(elements_path["description_tag"], "NaN")

										#DATES
										today = date.today()
										job_data["pubdate"] = today
										
										#locations
										job_data["location"] = job.get(elements_path["location_tag"], "NaN")
										
										#TIMESTAMP
										timestamp = datetime.now()
										job_data["timestamp"] = timestamp

										#Put it all together...
										total_links.append(job_data["link"])
										total_titles.append(job_data["title"])
										total_pubdates.append(job_data["pubdate"])
										total_locations.append(job_data["location"])
										total_timestamps.append(job_data["timestamp"])
										total_descriptions.append(job_data["description"])
						except json.JSONDecodeError:
							print(f"Failed to decode JSON for API: {api}. Skipping...")
							logging.error(f"Failed to decode JSON for API: {api}. Skipping...")
			except aiohttp.ClientError as e:
				print(f"Encountered a request error: {e}. Moving to the next API...")
				logging.error(f"Encountered a request error: {e}. Moving to the next API...")
				pass
		return rows

	async def gather_json_loads_api(session):
		with open(JSON) as f:
			data = json.load(f)
			apis = data[0]["apis"]

		tasks = [async_api_fetcher(session, api_obj) for api_obj in apis]
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
			logging.info("Async_API: LISTS HAVE THE SAME LENGHT. SENDING TO POSTGRE")
			clean_postgre_api(df = pd.DataFrame(combined_data), csv_path=SAVE_PATH, db=POSTGRESQL)
		else:
			logging.error(f"ERROR ON Async_API. LISTS DO NOT HAVE SAME LENGHT. FIX: \n {lengths_info}")
			pass

	async with aiohttp.ClientSession() as session:
		await gather_json_loads_api(session)
	
	elapsed_time = asyncio.get_event_loop().time() - start_time
	print(f"Async APIs finished! all in: {elapsed_time:.2f} seconds.", "\n")
	logging.info(f"Async APIs finished! all in: {elapsed_time:.2f} seconds.")
async def main():
	await async_api_template("TEST")

if __name__ == "__main__":
	asyncio.run(main())
