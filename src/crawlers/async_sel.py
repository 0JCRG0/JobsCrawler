# #!/usr/local/bin/python3

# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.common.exceptions import NoSuchElementException 
# from selenium.webdriver.support.wait import WebDriverWait
# from concurrent.futures import ThreadPoolExecutor
# import traceback
# from selenium.webdriver.chrome.service import Service
# import pandas as pd
# import pretty_errors
# import timeit
# from datetime import date, datetime
# import json
# from src.utils.logger_helper import get_custom_logger
# import os
# import asyncio
# from src.utils.FollowLink import *
# from dotenv import load_dotenv
# from src.utils.sel_utils import clean_postgre_sel
# from src.utils.handy import *


# """ LOAD THE ENVIRONMENT VARIABLES """

# load_dotenv()

# JSON_PROD = os.environ.get('JSON_PROD_SEL')
# JSON_TEST = os.environ.get('JSON_TEST_SEL')
# SAVE_PATH = os.environ.get('SAVE_PATH_SEL')
# LOGGER_PATH = os.environ.get("LOGGER_PATH", "")

# """ SET UP logger FILE """
# current_dir = os.path.dirname(os.path.abspath(__file__))

# logger_file_path = os.path.join(current_dir, LOGGER_PATH)



# async def async_selenium_template(pipeline):
# 	#start timer
# 	start_time = timeit.default_timer()

# 	#Modify the options so it is headless - to disable just comment the next 2 lines and use the commented driver
# 	service = Service()
# 	options = webdriver.ChromeOptions()
# 	#options.add_argument('--headless=new')
# 	#service = Service(executable_path='/Users/juanreyesgarcia/chromedriver', log_path=path.devnull)
# 	#service.start()

# 	loggerMasterCrawler()

# 	#DETERMINING WHICH JSON TO LOAD & WHICH POSTGRE TABLE WILL BE USED

# 	JSON = None
# 	POSTGRESQL = None

# 	if JSON_PROD and JSON_TEST:
# 		JSON, POSTGRESQL, URL_DB = test_or_prod(pipeline=pipeline, json_prod=JSON_PROD, json_test=JSON_TEST)

# 	# Check that JSON and POSTGRESQL have been assigned valid values
# 	if JSON is None or POSTGRESQL is None:
# 		logger.error("Error: JSON and POSTGRESQL must be assigned valid values.")
# 		return

# 	print("\n", "Async Sel has started")
# 	logger.info("Async Sel crawler deployed!")

# 	# Create a connection to the database & cursor to check for existent links
# 	conn = psycopg2.connect(URL_DB)
# 	cur = conn.cursor()

# 	async def fetch_sel(url, driver):
# 		loop = asyncio.get_event_loop()
# 		with ThreadPoolExecutor() as executor:
# 			await loop.run_in_executor(executor, driver.get, url)
# 		return driver.page_source

# 	async def async_sel_crawler(url_obj, options):
# 		#NEW DRIVER EACH ITERATION FOR SITE
# 		#driver = webdriver.Chrome(options=options)
# 		driver = webdriver.Chrome(options=options, service=service)

# 		#INITIALISE THE LISTS
# 		total_links = []
# 		total_titles = []
# 		total_pubdates = []
# 		total_locations = [] 
# 		total_descriptions = []
# 		total_timestamps = []
# 		rows = {"title": total_titles,
# 				"link": total_links,
# 				"description": total_descriptions,
# 				"pubdate": total_pubdates,
# 				"location": total_locations,
# 				"timestamp": total_timestamps}


# 		"""LOAD JSON VARIABLES"""
# 		name = url_obj['name']
# 		print("\n", f"{name} has started", "\n")
# 		url_prefix = url_obj['url']
# 		elements_path = url_obj['elements_path'][0]
# 		pages = url_obj['pages_to_crawl']
# 		start_point = url_obj['start_point']
# 		strategy = url_obj['strategy']
# 		follow_link = url_obj['follow_link']
# 		inner_link_tag = url_obj['inner_link_tag']

# 		""" SET THE WAITING STRATEGIES """
# 		# Add these lines before the for loop
# 		driver.implicitly_wait(1)
# 		wait = WebDriverWait(driver, 10)
# 		for i in range(start_point, pages + 1):

# 			url = url_prefix + str(i)
# 			# get the url
# 			try:
# 				await fetch_sel(url, driver) # type: ignore
# 				print(f"Crawling {url} with {strategy} strategy")

# 				""" IF LINKS ARE *NOT* IN THE SAME ELEMENT AS JOBS """
# 				if strategy == "main":
# 					#jobs = wait.until(EC.visibility_of_all_elements_located((By.CSS_SELECTOR,  elements_path["jobs_path"])))
# 					jobs = driver.find_elements(By.CSS_SELECTOR,  elements_path["jobs_path"])
# 					if jobs:
# 						try:
# 							default_descriptions = []
# 							for job in jobs:
# 								job_data = {}

# 								#TITLES
# 								title_element = job.find_element(By.CSS_SELECTOR, elements_path["title_path"])
# 								job_data["title"] = title_element.get_attribute("innerHTML") if title_element else "NaN"

# 								#LINKS
# 								link_element = job.find_element(By.CSS_SELECTOR, elements_path["link_path"])
# 								job_data["link"] = link_element.get_attribute("href") if link_element else "NaN"

# 								""" WHETHER THE LINK IS IN THE DB """
# 								if await link_exists_in_db(link=job_data["link"], cur=cur, pipeline=pipeline):
# 									#logger.info(f"""Link {job_data["link"]} already found in the db. Skipping... """)
# 									continue
								
# 								#PUBDATES - to simplify things & considering this snippet will be run daily datetime is the same day as the day this is running                       
# 								today = date.today()
# 								job_data["pubdate"] = today
									
# 								#LOCATIONS
# 								location_element = job.find_element(By.CSS_SELECTOR, elements_path["location_path"])
# 								job_data["location"]= location_element.get_attribute("innerHTML") if location_element else "NaN"
									
# 								#Timestamps
# 								timestamp = datetime.now()
# 								job_data["timestamp"] = timestamp

# 								# add all except description
# 								total_links.append(job_data["link"])
# 								total_titles.append(job_data["title"])
# 								total_pubdates.append(job_data["pubdate"])
# 								total_locations.append(job_data["location"])
# 								total_timestamps.append(job_data["timestamp"])
												
# 								""" HOW TO FOLLOW THE SCRAPED LINKS, OR AT ALL."""

# 								description_default = job.find_element(By.CSS_SELECTOR, elements_path["description_path"])
# 								default =description_default.get_attribute("innerHTML") if description_default else "NaN"
# 								job_data["description"]= default
# 								#Placeholder for default descriptions
# 								default_descriptions.append(default)
							
# 							"""FOLLOW THE SCRAPED LINKS OUTSIDE THE FOR LOOP"""
# 							if follow_link == "yes":
# 								for i, link in enumerate(total_links):
# 									description = await async_follow_link_sel(followed_link=link, inner_link_tag=inner_link_tag, driver=driver, fetch_sel=fetch_sel, default=default_descriptions[i])
# 									total_descriptions.append(description)
# 							else:
# 								# Get the default descriptions
# 								total_descriptions = default_descriptions
# 						except (NoSuchElementException, TimeoutException, Exception) as e:
# 							error_message = f"{type(e).__name__} **while** getting the elements in {url}. {traceback.format_exc()}"
# 							print(error_message)
# 							logger.error(f"{error_message}\n")
# 							continue
# 				elif strategy == "container":
# 					#Identify the container with all the jobs
# 					container = driver.find_element(By.CSS_SELECTOR,elements_path["jobs_path"])
# 					default_descriptions_container = []
# 					#container = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, elements_path["jobs_path"])))
# 					if container:
# 						try:
# 							# Identify the elements for each job
# 							job_elements = list(zip(
# 								container.find_elements(By.CSS_SELECTOR, elements_path["title_path"]),
# 								container.find_elements(By.CSS_SELECTOR, elements_path["link_path"]),
# 								container.find_elements(By.CSS_SELECTOR, elements_path["description_path"]),
# 								container.find_elements(By.CSS_SELECTOR, elements_path["location_path"]),
# 							))
							
# 							for title_element, link_element, description_element, location_element in job_elements:
# 								title = title_element.get_attribute("innerHTML") if title_element else "NaN"
# 								link = link_element.get_attribute("href") if link_element else "NaN"
# 								default_description = description_element.get_attribute("innerHTML") if description_element else "NaN"
# 								location = location_element.get_attribute("innerHTML") if location_element else "NaN"

# 								""" WHETHER THE LINK IS IN THE DB """
# 								if await link_exists_in_db(link=link, cur=cur, pipeline=pipeline):
# 									continue

# 								# Add the data for the current job to the lists
# 								total_titles.append(title)
# 								total_links.append(link)
# 								total_locations.append(location)
# 								total_pubdates.append(date.today())
# 								total_timestamps.append(datetime.now())

# 								""" HOW TO FOLLOW THE SCRAPED LINKS, OR AT ALL."""

# 							description_elements = container.find_elements(By.CSS_SELECTOR, elements_path["description_path"])
# 							default_descriptions_container = [i.get_attribute("innerHTML") if i else "NaN" for i in description_elements]
							
# 							if follow_link == "yes":
# 								for i, link in enumerate(total_links):
# 									description = await async_follow_link_sel(followed_link=link, inner_link_tag=inner_link_tag, driver=driver, fetch_sel=fetch_sel, default=default_descriptions_container[i])
# 									total_descriptions.append(description)
# 							else:
# 								total_descriptions = default_descriptions_container

# 						except (NoSuchElementException, TimeoutException, Exception) as e:
# 							error_message = f"{type(e).__name__} **while** getting the elements in {url}. {traceback.format_exc()}"
# 							print(error_message)
# 							logger.error(f"{error_message}\n")
# 							continue
# 			except (NoSuchElementException, TimeoutException, Exception) as e:
# 				error_message = f"{type(e).__name__} **before** getting the elements in {url}. {traceback.format_exc()}"
# 				print(error_message)
# 				logger.error(f"{error_message}\n")
# 				continue
# 		driver.quit()
# 		#service.stop()
# 		return rows 

# 	async def gather_tasks_selenium(options):
# 		with open(JSON) as f:
# 			data = json.load(f)
# 			urls = data[0]["urls"]
# 			tasks = [async_sel_crawler(url_obj, options) for url_obj in urls]
# 			results = await asyncio.gather(*tasks)
# 			# Combine the results
# 			combined_data = {
# 				"title": [],
# 				"link": [],
# 				"description": [],
# 				"pubdate": [],
# 				"location": [],
# 				"timestamp": [],
# 			}
# 			for result in results:
# 				for key in combined_data:
# 					combined_data[key].extend(result[key])
			
# 			print("Lengths of lists before creating DataFrame:")
# 			print("Titles:", len(combined_data["title"]))
# 			print("Links:", len(combined_data["link"]))
# 			print("Descriptions:", len(combined_data["description"]))
# 			print("Pubdates:", len(combined_data["pubdate"]))
# 			print("Locations:", len(combined_data["location"]))
# 			print("Timestamps:", len(combined_data["timestamp"]))

# 			return combined_data
	
# 	async def insert_postgre():
# 		data = await gather_tasks_selenium(options) # type: ignore
# 		data_dic = dict(data)
# 		df = pd.DataFrame.from_dict(data_dic, orient='index')
# 		df = df.transpose()

# 		# count the number of duplicate rows
# 		num_duplicates = df.duplicated(subset="title").sum()
# 		# print the number of duplicate rows
# 		print("Number of duplicate rows:", num_duplicates)
# 		# remove duplicate rows based on all columns
# 		df = df.drop_duplicates(subset="title")
# 		clean_postgre_sel(df=df, save_path=SAVE_PATH, function_postgre=POSTGRESQL)

# 	await insert_postgre()

# 	elapsed_time = asyncio.get_event_loop().time() - start_time
# 	print(f"Async Sel crawlers finished! all in: {elapsed_time:.2f} seconds.", "\n")
# 	logger.info(f"Async Sel finished! all in: {elapsed_time:.2f} seconds.")
	
# async def main():
# 	await async_selenium_template("PROD")

# if __name__ == "__main__":
# 	asyncio.run(main())