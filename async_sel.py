#!/usr/local/bin/python3

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException 
from selenium.common.exceptions import TimeoutException as TimeoutExceptionWebDriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor
import traceback
from os import path
from selenium.webdriver.chrome.service import Service
import pandas as pd
import re
import pretty_errors
from traceback import format_exc
import timeit
from dateutil.parser import parse
from datetime import date, datetime
import json
import logging
import os
import asyncio
import aiohttp
from utils.FollowLink import *
from dotenv import load_dotenv
from utils.sel_utils import clean_postgre_sel
from utils.handy import *


""" LOAD THE ENVIRONMENT VARIABLES """

load_dotenv()

PROD = os.environ.get('JSON_PROD_SEL')
TEST = os.environ.get('JSON_TEST_SEL')
SAVE_PATH = os.environ.get('SAVE_PATH_SEL')
user = os.environ.get('user')
password = os.environ.get('password')
host = os.environ.get('host')
port = os.environ.get('port')
database = os.environ.get('database')

# Create a connection to the database & cursor
conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
cur = conn.cursor()

async def async_selenium_template(pipeline):
	#start timer
	start_time = timeit.default_timer()

	#Modify the options so it is headless - to disable just comment the next 2 lines and use the commented driver
	options = webdriver.ChromeOptions()
	options.add_argument('--headless=new')
	#service = Service(executable_path='/Users/juanreyesgarcia/chromedriver', log_path=path.devnull)
	#service.start()

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

	print("\n", "Async Sel has started")
	logging.info("Async Sel crawler deployed!")

	async def fetch_sel(url, driver):
		loop = asyncio.get_event_loop()
		with ThreadPoolExecutor() as executor:
			await loop.run_in_executor(executor, driver.get, url)
		return driver.page_source

	async def async_sel_crawler(url_obj, options):
		#NEW DRIVER EACH ITERATION FOR SITE
		driver = webdriver.Chrome(options=options)
		#driver = webdriver.Chrome(options=options, service=service)

		#INITIALISE THE LISTS
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


		"""LOAD JSON VARIABLES"""
		name = url_obj['name']
		print("\n", f"{name} has started", "\n")
		url_prefix = url_obj['url']
		elements_path = url_obj['elements_path'][0]
		pages = url_obj['pages_to_crawl']
		start_point = url_obj['start_point']
		strategy = url_obj['strategy']
		follow_link = url_obj['follow_link']
		inner_link_tag = url_obj['inner_link_tag']

		""" SET THE WAITING STRATEGIES """
		# Add these lines before the for loop
		driver.implicitly_wait(1)
		wait = WebDriverWait(driver, 10)
		for i in range(start_point, pages + 1):

			url = url_prefix + str(i)
			# get the url
			try:
				await fetch_sel(url, driver) # type: ignore
				print(f"Crawling {url} with {strategy} strategy")

				""" IF LINKS ARE *NOT* IN THE SAME ELEMENT AS JOBS """
				if strategy == "main":
					#jobs = wait.until(EC.visibility_of_all_elements_located((By.CSS_SELECTOR,  elements_path["jobs_path"])))
					jobs = driver.find_elements(By.CSS_SELECTOR,  elements_path["jobs_path"])
					if jobs:
						try:
							default_descriptions = []
							for job in jobs:
								job_data = {}

								#TITLES
								title_element = job.find_element(By.CSS_SELECTOR, elements_path["title_path"])
								job_data["title"] = title_element.get_attribute("innerHTML") if title_element else "NaN"

								#LINKS
								link_element = job.find_element(By.CSS_SELECTOR, elements_path["link_path"])
								job_data["link"] = link_element.get_attribute("href") if link_element else "NaN"

								""" WHETHER THE LINK IS IN THE DB """
								if await link_exists_in_db(link=job_data["link"], cur=cur):
									#logging.info(f"""Link {job_data["link"]} already found in the db. Skipping... """)
									continue
								
								#PUBDATES - to simplify things & considering this snippet will be run daily datetime is the same day as the day this is running                       
								today = date.today()
								job_data["pubdate"] = today
									
								#LOCATIONS
								location_element = job.find_element(By.CSS_SELECTOR, elements_path["location_path"])
								job_data["location"]= location_element.get_attribute("innerHTML") if location_element else "NaN"
									
								#Timestamps
								timestamp = datetime.now()
								job_data["timestamp"] = timestamp

								# add all except description
								total_links.append(job_data["link"])
								total_titles.append(job_data["title"])
								total_pubdates.append(job_data["pubdate"])
								total_locations.append(job_data["location"])
								total_timestamps.append(job_data["timestamp"])
												
								""" HOW TO FOLLOW THE SCRAPED LINKS, OR AT ALL."""

								description_default = job.find_element(By.CSS_SELECTOR, elements_path["description_path"])
								default =description_default.get_attribute("innerHTML") if description_default else "NaN"
								job_data["description"]= default
								#Placeholder for default descriptions
								default_descriptions.append(default)
							
							"""FOLLOW THE SCRAPED LINKS OUTSIDE THE FOR LOOP"""
							if follow_link == "yes":
								for i, link in enumerate(total_links):
									description = await async_follow_link_sel(followed_link=link, inner_link_tag=inner_link_tag, driver=driver, fetch_sel=fetch_sel, default=default_descriptions[i])
									total_descriptions.append(description)
							else:
								# Get the default descriptions
								total_descriptions = default_descriptions
						except (NoSuchElementException, TimeoutException, Exception) as e:
							error_message = f"{type(e).__name__} **while** getting the elements in {url}. {traceback.format_exc()}"
							print(error_message)
							logging.error(f"{error_message}\n")
							continue
				elif strategy == "container":
					#Identify the container with all the jobs
					container = driver.find_element(By.CSS_SELECTOR,elements_path["jobs_path"])
					default_descriptions_container = []
					#container = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, elements_path["jobs_path"])))
					if container:
						try:
							# Identify the elements for each job
							job_elements = list(zip(
								container.find_elements(By.CSS_SELECTOR, elements_path["title_path"]),
								container.find_elements(By.CSS_SELECTOR, elements_path["link_path"]),
								container.find_elements(By.CSS_SELECTOR, elements_path["description_path"]),
								container.find_elements(By.CSS_SELECTOR, elements_path["location_path"]),
							))
							
							for title_element, link_element, description_element, location_element in job_elements:
								title = title_element.get_attribute("innerHTML") if title_element else "NaN"
								link = link_element.get_attribute("href") if link_element else "NaN"
								default_description = description_element.get_attribute("innerHTML") if description_element else "NaN"
								location = location_element.get_attribute("innerHTML") if location_element else "NaN"

								""" WHETHER THE LINK IS IN THE DB """
								if await link_exists_in_db(link=link, cur=cur):
									continue

								# Add the data for the current job to the lists
								total_titles.append(title)
								total_links.append(link)
								total_locations.append(location)
								total_pubdates.append(date.today())
								total_timestamps.append(datetime.now())

								""" HOW TO FOLLOW THE SCRAPED LINKS, OR AT ALL."""

							description_elements = container.find_elements(By.CSS_SELECTOR, elements_path["description_path"])
							default_descriptions_container = [i.get_attribute("innerHTML") if i else "NaN" for i in description_elements]
							
							if follow_link == "yes":
								for i, link in enumerate(total_links):
									description = await async_follow_link_sel(followed_link=link, inner_link_tag=inner_link_tag, driver=driver, fetch_sel=fetch_sel, default=default_descriptions_container[i])
									total_descriptions.append(description)
							else:
								total_descriptions = default_descriptions_container

						except (NoSuchElementException, TimeoutException, Exception) as e:
							error_message = f"{type(e).__name__} **while** getting the elements in {url}. {traceback.format_exc()}"
							print(error_message)
							logging.error(f"{error_message}\n")
							continue
			except (NoSuchElementException, TimeoutException, Exception) as e:
				error_message = f"{type(e).__name__} **before** getting the elements in {url}. {traceback.format_exc()}"
				print(error_message)
				logging.error(f"{error_message}\n")
				continue
		driver.quit()
		#service.stop()
		return rows 

	async def gather_tasks_selenium(options):
		with open(JSON) as f:
			data = json.load(f)
			urls = data[0]["urls"]
			tasks = [async_sel_crawler(url_obj, options) for url_obj in urls]
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
			
			print("Lengths of lists before creating DataFrame:")
			print("Titles:", len(combined_data["title"]))
			print("Links:", len(combined_data["link"]))
			print("Descriptions:", len(combined_data["description"]))
			print("Pubdates:", len(combined_data["pubdate"]))
			print("Locations:", len(combined_data["location"]))
			print("Timestamps:", len(combined_data["timestamp"]))

			return combined_data
	
	async def insert_postgre():
		data = await gather_tasks_selenium(options) # type: ignore
		data_dic = dict(data)
		df = pd.DataFrame.from_dict(data_dic, orient='index')
		df = df.transpose()

		# count the number of duplicate rows
		num_duplicates = df.duplicated(subset="title").sum()
		# print the number of duplicate rows
		print("Number of duplicate rows:", num_duplicates)
		# remove duplicate rows based on all columns
		df = df.drop_duplicates(subset="title")
		clean_postgre_sel(df=df, csv_path=SAVE_PATH, db=POSTGRESQL)

	await insert_postgre()

	elapsed_time = asyncio.get_event_loop().time() - start_time
	print(f"Async BS4 crawlers finished! all in: {elapsed_time:.2f} seconds.", "\n")
	logging.info(f"Async Sel finished! all in: {elapsed_time:.2f} seconds.")
	
async def main():
	await async_selenium_template("TEST")

if __name__ == "__main__":
	asyncio.run(main())