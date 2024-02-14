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
import random
import aiohttp
""" LOAD THE ENVIRONMENT VARIABLES """

#TODO: Move Himalayas to API

load_dotenv()

JSON_PROD = os.environ.get('JSON_PROD_BS4')
JSON_TEST = os.environ.get('JSON_TEST_BS4')
SAVE_PATH = os.environ.get('SAVE_PATH_BS4')
user = os.environ.get('user')
password = os.environ.get('password')
host = os.environ.get('host')
port = os.environ.get('port')
database = os.environ.get('database')


async def async_bs4_template(pipeline):

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

	# Create a connection to the database & cursor to check for existent links
	conn = psycopg2.connect(URL_DB)
	cur = conn.cursor()

	async def fetch(url, session):
		#Get a random header agent from the pool of headers
		random_user_agent = {'User-Agent': random.choice(user_agents)}
		async with session.get(url, headers=random_user_agent) as response:
			response_text = response.text
			logging.debug(f"random_header: {random_user_agent}\nresponse_text: {response_text}")
			return await response.text()

	async def async_bs4_crawler(session, url_obj):
		
		# Initialize the rows dictionary with lists for each key
		rows = {
			"title": [],
			"link": [],
			"description": [],
			"pubdate": [],
			"location": [],
			"timestamp": []
		}

		name = url_obj['name']
		url_prefix = url_obj['url']
		elements_path = url_obj['elements_path'][0]
		pages = url_obj['pages_to_crawl']
		start_point = url_obj['start_point']
		strategy = url_obj['strategy']
		follow_link = url_obj['follow_link']
		inner_link_tag = url_obj['inner_link_tag']

		logging.info(f"{name} has started")
		logging.debug(f"All parameters for {name}:\n{url_obj}")
		
		async with aiohttp.ClientSession() as session:

				for i in range(start_point, pages + 1):
					url = url_prefix + str(i)

					try:
						html = await fetch(url, session)
						soup = bs4.BeautifulSoup(html, 'lxml')
						logging.info(f"Crawling {url} with {strategy} strategy")
						
						if strategy == "main":
							try:
								new_rows = await async_main_strategy_bs4(pipeline, cur, session, elements_path, name, inner_link_tag, follow_link, soup)
							except Exception as e:
								error_message = f"{type(e).__name__} in **async_main_strategy_bs4()** while crawling {url}.\n\n{e}"
								logging.error(f"{error_message}\n", exc_info=True)
								continue
						
						elif strategy == "container":
							try:
								new_rows = await async_container_strategy_bs4(pipeline, cur, session, elements_path, name, inner_link_tag, follow_link, soup)
							except Exception as e:
								error_message = f"{type(e).__name__} in **async_container_strategy_bs4()** while crawling {url}.\n\n{e}"
								logging.error(f"{error_message}\n", exc_info=True)
								continue
						elif strategy == "occ":
							try:
								new_rows = await async_occ_mundial(pipeline, cur, session, elements_path, name, inner_link_tag, follow_link, soup)
							except Exception as e:
								error_message = f"{type(e).__name__} in **async_occ_mundial()** while crawling {url}.\n\n{e}"
								logging.error(f"{error_message}\n", exc_info=True)
								continue
						# Update the rows dictionary with the new data
						for key in rows:
							rows[key].extend(new_rows.get(key, []))
					
					except Exception as e:
						error_message = f"{type(e).__name__} occured before deploying crawling strategy on {url}.\n\n{e}"
						logging.error(f"{error_message}\n", exc_info=True)
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
	await async_bs4_template("TEST")

if __name__ == "__main__":
	asyncio.run(main())