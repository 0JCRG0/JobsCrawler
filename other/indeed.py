#!/usr/local/bin/python3

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
import pandas as pd
import re
import pretty_errors
import json
import timeit
from datetime import date, datetime
from dotenv import load_dotenv
import os
import sys

#TODO: Add logging

""" LOAD THE ENVIRONMENT VARIABLES """

load_dotenv()

PROD = os.environ.get('JSON_PROD_INDEED', "")
TEST = os.environ.get('JSON_TEST_INDEED', "")
SAVE_PATH = os.environ.get('SAVE_PATH_INDEED', "")
UTILS_PATH = os.environ.get('SYS_PATH_APPEND', "")

""" APPEND UTILS' PATH """
sys.path.append(UTILS_PATH)
from handy import *

#Initiate Logging
#LoggingMasterCrawler()

def indeed(SCHEME, KEYWORD):
    start_time = timeit.default_timer()

    options = webdriver.FirefoxOptions()
    options.add_argument('-headless')
    driver = webdriver.Firefox(options=options)

    #print("\n", f"Reading {file}... ", "\n")
    print("\n", "Crawler launched on INDEED.")

    """ LOAD JSON """
    with open(PROD) as f:
        data = json.load(f)
        strategies = [strategy for strategy in data[0]["strategies"] if strategy["strategy_name"] == SCHEME]

    if strategies:
        strategy = strategies[0]
        country_code = strategy["code"]
        special_url = strategy["special_url"]
        url_1 = strategy["url_1"]
        url_2 = strategy["url_2"]
        pages_to_crawl = strategy["pages_to_crawl"]
        elements_path = strategy["elements_path"][0]
    
        def crawling():
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
                
            for i in range(0, pages_to_crawl * 10, 10):
                page_print = round(i/10) + 1
                if special_url == "yes":
                    url = url_1 + KEYWORD + url_2 + str(i)
                    print("\n", f"Crawler deployed on Indeed using {SCHEME} strategy. Looking for \"{KEYWORD}\". Currently crawling page number: {page_print}.")
                else:
                    url = strategy['url_1'] + str(i)
                    print("\n", f"Crawler deployed on Indeed using {SCHEME} strategy. Currently crawling page number: {page_print}.")
                    #Make the request
                try:
                    driver.get(url)
                    print(url)
                    #Set waiting strategy
                    driver.implicitly_wait(1.5)
                    jobs = driver.find_elements(By.CSS_SELECTOR, elements_path["jobs_path"])    
                    for job in jobs:
                        ##Get the attributes...
                        job_data = {}
                            
                        title_raw = job.find_element(By.CSS_SELECTOR, elements_path["title_path"])
                        job_data["title"] = title_raw.get_attribute("innerHTML") if title_raw else "NaN"
                            
                        #LINKS
                        link_raw = job.find_element(By.CSS_SELECTOR, elements_path["link_path"])
                        job_data["link"] = link_raw.get_attribute("href") if link_raw else "NaN"

                        #DATES
                        today = date.today()
                        job_data["pubdate"] = today
                            
                        #LOCATION    
                        location_raw = job.find_element(By.CSS_SELECTOR, elements_path["location_path"])
                        job_data["location"] = location_raw.text if location_raw else "NaN"

                        #DESCRIPTIONS
                        description_raw = job.find_element(By.CSS_SELECTOR, elements_path["description_path"])
                        job_data["description"] = description_raw.get_attribute("innerHTML") if description_raw else "NaN"

                        #TIMESTAMP
                        timestamp = datetime.now()
                        job_data["timestamp"] = timestamp

                        #Put it all together...
                        total_titles.append(job_data["title"])
                        total_links.append(job_data["link"])
                        total_pubdates.append(job_data["pubdate"])
                        total_locations.append(job_data["location"])
                        total_descriptions.append(job_data["description"])
                        total_timestamps.append(job_data["timestamp"])
                except NoSuchElementException as e:
                # Handle the specific exception
                    print("Element not found:", e)
                    #logging.error("Element not found:", e)
                    continue
                except Exception as e:
                    print("\n", f"WARNING! Exception: {e}", "\n")
                    #logging.error("\n", f"WARNING! Exception: {e}", "\n")
                    continue
            return rows

        data = crawling()
        driver.quit()

        #Convert data to a pandas df for further analysis
        df = pd.DataFrame(data)

        df.to_csv(SAVE_PATH, index=False)


        for col in df.columns:
            if col == 'description':
                df[col] = df[col].apply(cleansing_selenium_crawlers)
                #df[col] = ["{MX}".format(i) for i in df[col]]
            elif col == 'location':
                df[col] = df[col] + str(country_code)
        
        #logging.info('Finished Indeed. Results below ⬇︎')

        # SEND IT TO TO PostgreSQL
        to_postgre(df)

        
        #stop the timer
        elapsed_time = timeit.default_timer() - start_time
        print("\n", f"Finished crawling Indeed. All in {elapsed_time:.5f} seconds!!!", "\n")
    else:
        print("\n", """INCORRECT ARGUMENT! CHOOSE "main_mx", "specific_mx" or "specific_usa" """, "\n")


    
if __name__ == "__main__":
    indeed("main_mx", "") 
    """
    1st = "main_mx", "specific_mx" or "specific_usa"
    2nd = KEYWORD if specific if not empty string
    
    """