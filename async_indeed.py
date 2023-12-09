#!/usr/local/bin/python3

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from os import path
from selenium.webdriver.chrome.service import Service
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import re
import pretty_errors
import json
import asyncio
import timeit
from utils.indeed_utils import *
from utils.FollowLink import *
from datetime import date, datetime
from dotenv import load_dotenv
import os
import sys
from traceback import format_exc
from utils.handy import *

""" LOAD THE ENVIRONMENT VARIABLES """

load_dotenv()

JSON_PROD = os.environ.get('JSON_PROD_INDEED', "")
JSON_TEST = os.environ.get('JSON_TEST_INDEED', "")
SAVE_PATH = os.environ.get('SAVE_PATH_INDEED', "")

#TODO Needs a lot of fixes. Mainly headless browser & that all arrays must be the same lenght. Also, it does not respect the follow link

async def async_indeed_template(SCHEME, KEYWORD, pipeline):
	start_time = timeit.default_timer()
	#Setting up options for the WebDriver
	#options = webdriver.ChromeOptions()
	#options.add_argument('--headless=new')
	#service = Service(executable_path='/Users/juanreyesgarcia/chromedriver')
	#service = Service()
	#Fucking start it ffs
	#service.start()

	""" DETERMINING WHICH JSON TO LOAD & WHICH POSTGRE TABLE WILL BE USED """

	LoggingMasterCrawler()

	#DETERMINING WHICH JSON TO LOAD & WHICH POSTGRE TABLE WILL BE USED

	JSON = None
	POSTGRESQL = None

	if JSON_PROD and JSON_TEST:
		JSON, POSTGRESQL, URL_DB = test_or_prod(pipeline=pipeline, json_prod=JSON_PROD, json_test=JSON_TEST)

	# Check that JSON and POSTGRESQL have been assigned valid values
	if JSON is None or POSTGRESQL is None:
		logging.error("Error: JSON and POSTGRESQL must be assigned valid values.")
		return
	

	print("\n", "Async INDEED has started.")
	logging.info("Async INDEED crawler deployed!.")

	# Create a connection to the database & cursor to check for existent links
	conn = psycopg2.connect(URL_DB)
	cur = conn.cursor()

	async def fetch_indeed(url, driver):
		loop = asyncio.get_event_loop()
		with ThreadPoolExecutor() as executor:
			await loop.run_in_executor(executor, driver.get, url)
		return driver.page_source
	
	#async def async_indeed_crawling(strategies, options):
	async def async_indeed_crawling(strategies):

		#NEW DRIVER EACH ITERATION FOR SITE
		#driver = webdriver.Chrome(options=options, service=service)
		#driver = webdriver.Chrome(options=options)
		driver = webdriver.Chrome()

		total_titles = []
		total_links = []
		total_pubdates = []
		total_locations = [] 
		total_descriptions = []
		total_timestamps = []
		rows = {'title': total_titles,
				'link': total_links,
				'description': total_descriptions,
				'pubdate': total_pubdates,
				'location': total_locations,
				"timestamp": total_timestamps}
		strategy = strategies[0]
		country_code = strategy["code"]
		special_url = strategy["special_url"]
		url_1 = strategy["url_1"]
		url_2 = strategy["url_2"]
		pages_to_crawl = strategy["pages_to_crawl"]
		elements_path = strategy["elements_path"][0]
		follow_link = strategy['follow_link']
		inner_link_tag = strategy['inner_link_tag']

		""" SET THE WAITING STRATEGIES """
		# Add these lines before the for loop
		driver.implicitly_wait(1.5)
		#wait = WebDriverWait(driver, 10)

		for i in range(0, pages_to_crawl * 10, 10):
			page_print = round(i/10) + 1
			if special_url == "yes":
				url = url_1 + KEYWORD + url_2 + str(i)
				operation = f"requesting {url}"
				print("\n", f"Crawler deployed on Indeed using {SCHEME} strategy. Looking for \"{KEYWORD}\". Currently crawling page number: {page_print}.")
			else:
				url = strategy['url_1'] + str(i)
				operation = f"requesting {url}"
				print("\n", f"Crawler deployed on Indeed using {SCHEME} strategy. Currently crawling page number: {page_print}.", "\n")
				#Make the request
			try:
				await fetch_indeed(url, driver)
				print(f"Crawling {url}...")
				jobs = driver.find_elements(By.CSS_SELECTOR, elements_path["jobs_path"])
				
				#jobs = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, elements_path["jobs_path"])))
				#jobs = wait.until(EC.visibility_of_all_elements_located((By.CSS_SELECTOR, elements_path["jobs_path"])))
				try:
					if jobs is not None:
						try:
							default_descriptions = []
							for job in jobs:
								operation = f"getting elements from indeed in this link:"
								##Get the attributes...
								job_data = {}
									
								title_raw = job.find_element(By.CSS_SELECTOR, elements_path["title_path"])
								job_data["title"] = title_raw.get_attribute("innerHTML") if title_raw else "NaN"
									
								#LINKS
								link_raw = job.find_element(By.CSS_SELECTOR, elements_path["link_path"])
								job_data["link"] = link_raw.get_attribute("href") if link_raw else "NaN"

								""" WHETHER THE LINK IS IN THE DB """
								if await link_exists_in_db(link=job_data["link"], cur=cur, pipeline=pipeline):
									continue

								#DATES
								today = date.today()
								job_data["pubdate"] = today
									
								#LOCATION    
								location_raw = job.find_element(By.CSS_SELECTOR, elements_path["location_path"])
								job_data["location"] = location_raw.text if location_raw else "NaN"

								#TIMESTAMP
								timestamp = datetime.now()
								job_data["timestamp"] = timestamp

								#Put it all together...
								total_titles.append(job_data["title"])
								total_links.append(job_data["link"])
								total_pubdates.append(job_data["pubdate"])
								total_locations.append(job_data["location"])
								total_timestamps.append(job_data["timestamp"])

								description_raw = job.find_element(By.CSS_SELECTOR, elements_path["description_path"])
								default = description_raw.get_attribute("innerHTML") if description_raw else "NaN"
								job_data["description"] = default
								#This is a placeholder for default descriptions
								default_descriptions.append(default)
														
							"""FOLLOW THE SCRAPED LINKS OUTSIDE THE FOR LOOP"""
							if follow_link == "yes":
								for i, link in enumerate(total_links):
									if i < len(default_descriptions):
											description = await async_follow_link_indeed(followed_link=link, inner_link_tag=inner_link_tag, driver=driver, fetch_sel=fetch_indeed, default=default_descriptions[i])
											total_descriptions.append(description)
									else:
										logging.warning(f"IndexError avoided. Index: {i} is bigger than the lenght of default descriptions: {len(default_descriptions)}")
										continue
							else:
								# Get the default descriptions
								total_descriptions = default_descriptions
						except (NoSuchElementException, TimeoutException, Exception) as e:
							error_message = f"{type(e).__name__} **while** {operation} {url}. {traceback.format_exc()}"
							print(error_message)
							logging.error(f"{error_message}\n")
							continue
				except (NoSuchElementException, TimeoutException, Exception) as e:
					error_message = f"**Jobs were not found** {type(e).__name__} {operation} {url}. {traceback.format_exc()}"
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
	
	#async def gather_tasks_indeed(options):
	async def gather_tasks_indeed():
		with open(JSON) as f:
			data = json.load(f)
			strategies = [strategy for strategy in data[0]["strategies"] if strategy["strategy_name"] == SCHEME]
			#tasks = [async_indeed_crawling(strategies, options)]
			tasks = [async_indeed_crawling(strategies)]
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

			clean_postgre_indeed(df=pd.DataFrame(combined_data), save_path=SAVE_PATH, function_postgre=POSTGRESQL, c_code=SCHEME)

	#await gather_tasks_indeed(options=options)
	await gather_tasks_indeed()

	
	#stop the timer
	elapsed_time = timeit.default_timer() - start_time
	print("\n", f"Finished async Indeed. All in {elapsed_time:.5f} seconds!!!", "\n")
	logging.info(f"Finished async Indeed. All in {elapsed_time:.5f} seconds!!!")
async def main():
	await async_indeed_template("USA", "", "TEST")

if __name__ == "__main__":
	asyncio.run(main())
	"""
	1st = "MX", "specific_mx" or "specific_usa"
	2nd = KEYWORD if specific if not empty string
	
	"""