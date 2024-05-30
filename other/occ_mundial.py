from bs4 import BeautifulSoup
from datetime import date, datetime
import pandas as pd
import random
from dotenv import load_dotenv
from utils.handy import *
from utils.bs4_utils import *
import os
import logging
import aiohttp

load_dotenv(".env")

JSON_PROD = os.environ.get('JSON_PROD_BS4')
JSON_TEST = os.environ.get('JSON_TEST_BS4')
SAVE_PATH = os.environ.get('SAVE_PATH_BS4')
OCC_JOB_COUNTER_TXT = os.environ.get("OCC_JOB_COUNTER_TXT")

async def async_occ_mundial_template(pipeline):

	#start timer
	start_time = asyncio.get_event_loop().time()

	""" DETERMINING WHICH JSON TO LOAD & WHICH POSTGRE TABLE WILL BE USED """

	LoggingMasterCrawler()

	#DETERMINING WHICH JSON TO LOAD & WHICH POSTGRE TABLE WILL BE USED

	JSON = None
	POSTGRESQL = None

	if JSON_PROD and JSON_TEST:
		JSON, POSTGRESQL, URL_DB = test_or_prod(pipeline=pipeline, json_prod=JSON_PROD, json_test=JSON_TEST)

	# Check that JSON and POSTGRESQL have been assigned valid values
	assert JSON is not None and POSTGRESQL is not None and URL_DB is not None, "JSON, POSTGRESQL, and URL_DB must be assigned valid values."

	logging.info("Async BS4 crawler deployed!.")

	async def fetch(url, session):
		random_user_agent = {'User-Agent': random.choice(USER_AGENTS)}
		async with session.get(url, headers=random_user_agent) as response:
			response_text = response.text
			logging.debug(f"random_header: {random_user_agent}\nresponse_text: {response_text}")
			return await response.text()

	async def async_occ_crawler():

		wfh_path = "a[href^=\"/empleos/tipo\"]"
		title_path = ".flex-0-2-5.jbetween-0-2-17.astart-0-2-20.titleRow-0-2-516 p"
		description_path = "#jobbody"
		default = "NaN"
		max_none_count = 15
		none_count = 0
		crawled_job_posts = 0

		with open(f"{OCC_JOB_COUNTER_TXT}", "r") as f:
			JOB_COUNTER = int(f.read())

		total_links = []
		total_titles = []
		total_pubdates = []
		total_locations = []
		total_descriptions = []
		total_timestamps = []


		# Start the loop
		while none_count <= max_none_count or crawled_job_posts >= 400:
			async with aiohttp.ClientSession() as session:
				try:
					url = f"https://www.occ.com.mx/empleo/oferta/{JOB_COUNTER}/"
					html = await fetch(url, session)
					#r = requests.get(url)
					soup = BeautifulSoup(html, 'html.parser')
					logging.info(f"Crawling {url}")
					
					wfh = soup.select_one(wfh_path)

					if crawled_job_posts in (100, 200, 300, 400):
						await asyncio.sleep(random.randint(1, 3))

					if wfh is not None:
						wfh_final = wfh.text.strip() if wfh else default
						# Check if the text matches "Desde casa"
						if wfh_final == "Desde casa":
							print(f"wfh_final == {wfh_final}" )
							title_tag = soup.select_one(title_path)
							description_tag = soup.select_one(description_path)
							title_final = title_tag.text if title_tag else default
							print(title_final)
							description_final = description_tag.text if description_tag else default
							print(description_final)

							total_titles.append(title_final)
							total_links.append(url)
							total_descriptions.append(description_final)
							total_locations.append("MX")
							total_pubdates.append(date.today())
							total_timestamps.append(datetime.now())
					else:
						none_count += 1 

					# Update the job counter regardless of whether the job is "Desde casa" or not
					JOB_COUNTER += 1
					crawled_job_posts += 1
					with open(OCC_JOB_COUNTER_TXT, "w") as f:
						f.write(str(JOB_COUNTER))
				except Exception as e:
					error_message = f"{type(e).__name__} occured before crawling on {url}.\n\n{e}"
					logging.error(f"{error_message}\n", exc_info=True)
					continue


		return {
				"title": total_titles,
				"link": total_links,
				"description": total_descriptions,
				"pubdate": total_pubdates,
				"location": total_locations,
				"timestamp": total_timestamps
			}
	
	async def gather_json_loads_bs4(session):
		with open(JSON) as f:
			data = json.load(f)
			urls = data[0]["urls"]

		tasks = [async_occ_crawler(session, url_obj) for url_obj in urls]
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
			clean_postgre_bs4(df=pd.DataFrame(combined_data), save_data_path=SAVE_PATH, function_postgre=POSTGRESQL)
		else:
			logging.error(f"ERROR ON ASYNC BS4. LISTS DO NOT HAVE SAME LENGHT. FIX {lengths_info}")
			pass
	
	async with aiohttp.ClientSession() as session:
		await gather_json_loads_bs4(session)

	elapsed_time = asyncio.get_event_loop().time() - start_time
	print(f"Async BS4 crawlers finished! all in: {elapsed_time:.2f} seconds.", "\n")
	logging.info(f"Async BS4 crawlers finished! all in: {elapsed_time:.2f} seconds.")

async def main():
	await async_occ_mundial_template("TEST")