#!/usr/local/bin/python3

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from concurrent.futures import ThreadPoolExecutor
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from httpcore import TimeoutException
import pandas as pd
import re
import pretty_errors
import timeit
from dateutil.parser import parse
from datetime import date, datetime
import json
import logging
import os
import asyncio
import aiohttp
from dotenv import load_dotenv
from utils.sel_utils import clean_postgre_sel
from utils.handy import *


""" LOAD THE ENVIRONMENT VARIABLES """

load_dotenv()

PROD = os.environ.get('JSON_PROD_SEL')
TEST = os.environ.get('JSON_TEST_SEL', "")
SAVE_PATH = os.environ.get('SAVE_PATH_SEL')

with open(TEST) as f:
	data = json.load(f)
	urls = data[0]["urls"]
	for url_obj in urls:
		inner_link_tag = url_obj['inner_link_tag']

def sel_test():
	#Modify the options so it is headless - to disable just comment the next 2 lines and use the commented driver
	options = webdriver.FirefoxOptions()
	options.add_argument('-headless')
		
	# Start the session
	driver = webdriver.Firefox(options=options)
	#url = "https://cryptojobslist.com/marketing/community-manager-korea-manta-network-powered-by-p0x-labs-any"
	url = "https://www.remoteimpact.io/jobs/site-reliability-engineer-traffic-wikimedia-s8am19"
	#driver.get(url)
	driver.get(url)
	wait = WebDriverWait(driver, 10)
		# Wait for the element to be present in the DOM and to be visible
	try:
		description_tag = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, inner_link_tag)))
		description_final = description_tag.get_attribute("innerHTML") if description_tag else "NaN"
		print(description_final)
	except TimeoutException:
			print("Element not found within the specified wait time.")
			description_final = "NaN"
			print(description_final)

if __name__ == "__main__":
	sel_test()