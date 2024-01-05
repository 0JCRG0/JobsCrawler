from typing import Callable
from psycopg2.extensions import cursor
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
from utils.handy import * #This includes link_exists_in_db
from utils.bs4_utils import *
import asyncio
import aiohttp


async def async_main_strategy_bs4(pipeline:str, cur: cursor, session: aiohttp.ClientSession, elements_path: dict, name:str, inner_link_tag:str, follow_link:str, soup: bs4.BeautifulSoup):
	
	total_links = []
	total_titles = []
	total_pubdates = []
	total_locations = []
	total_descriptions = []
	total_timestamps = []
	

	jobs = soup.select(elements_path["jobs_path"])
	assert jobs is not None, "No elements found for 'jobs' in main_strategy_bs4(). Check 'elements_path[\"jobs_path\"]'"
	for job in jobs:

		# create a new dictionary to store the data for the current job
		job_data = {}

		title_element = job.select_one(elements_path["title_path"])
		assert title_element is not None, "No elements found for 'title_element' in main_strategy_bs4(). Check 'elements_path[\"title_path\"]'"
		job_data["title"] = title_element.text if title_element else "NaN"


		link_element = job.select_one(elements_path["link_path"])
		assert link_element is not None, "No elements found for 'link_element' in main_strategy_bs4(). Check 'elements_path[\"link_path\"]'"
		job_data["link"] = name + link_element["href"] if link_element else "NaN"

		""" WHETHER THE LINK IS IN THE DB """
		if await link_exists_in_db(link=job_data["link"], cur=cur, pipeline=pipeline):
			logging.info(f"""Link {job_data["link"]} already found in the db. Skipping... """)
			continue
		else:
			""" WHETHER TO FOLLOW LINK """
			description_default = job.select_one(elements_path["description_path"])
			default = description_default.text if description_default else "NaN"
			if follow_link == "yes":
				job_data["description"] = ""
				job_data["description"] = await async_follow_link(session=session, followed_link=job_data['link'], description_final=job_data["description"], inner_link_tag=inner_link_tag, default=default)
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

	return {
		"title": total_titles,
		"link": total_links,
		"description": total_descriptions,
		"pubdate": total_pubdates,
		"location": total_locations,
		"timestamp": total_timestamps
	}



def clean_postgre_bs4(df: pd.DataFrame, save_data_path: str, function_postgre: Callable):
	#df = pd.DataFrame(data) # type: ignore
	"""data_dic = dict(data) # type: ignore
	df = pd.DataFrame.from_dict(data_dic, orient='index')
	df = df.transpose()"""

		# count the number of duplicate rows
	num_duplicates = df.duplicated().sum()

			# print the number of duplicate rows
	print("Number of duplicate rows:", num_duplicates)

# remove duplicate rows based on all columns
	df = df.drop_duplicates()
		
		#CLEANING AVOIDING DEPRECATION WARNING
	for col in df.columns:
		if col == 'title' or col == 'description':
			if not df[col].empty:  # Check if the column has any rows
				df[col] = df[col].astype(str)  # Convert the entire column to string
				df[col] = df[col].str.replace(r'<.*?>|[{}[\]\'",]', '', regex=True) #Remove html tags & other characters
		elif col == 'location':
			if not df[col].empty:  # Check if the column has any rows
				df[col] = df[col].astype(str)  # Convert the entire column to string
				df[col] = df[col].str.replace(r'<.*?>|[{}[\]\'",]', '', regex=True) #Remove html tags & other characters
				#df[col] = df[col].str.replace(r'[{}[\]\'",]', '', regex=True)
				df[col] = df[col].str.replace(r'\b(\w+)\s+\1\b', r'\1', regex=True) # Removes repeated words
				df[col] = df[col].str.replace(r'\d{4}-\d{2}-\d{2}', '', regex=True)  # Remove dates in the format "YYYY-MM-DD"
				df[col] = df[col].str.replace(r'(USD|GBP)\d+-\d+/yr', '', regex=True)  # Remove USD\d+-\d+/yr or GBP\d+-\d+/yr.
				df[col] = df[col].str.replace('[-/]', ' ', regex=True)  # Remove -
				df[col] = df[col].str.replace(r'(?<=[a-z])(?=[A-Z])', ' ', regex=True)  # Insert space between lowercase and uppercase letters
				pattern = r'(?i)\bRemote Job\b|\bRemote Work\b|\bRemote Office\b|\bRemote Global\b|\bRemote with frequent travel\b'     # Define a regex patter for all outliers that use remote 
				df[col] = df[col].str.replace(pattern, 'Worldwide', regex=True)
				df[col] = df[col].replace('(?i)^remote$', 'Worldwide', regex=True) # Replace 
				df[col] = df[col].str.strip()  # Remove trailing white space
		
		#Save it in local machine
	df.to_csv(save_data_path, index=False)

		#Log it 
	logging.info('Finished bs4 crawlers. Results below ⬇︎')
		
		# SEND IT TO TO PostgreSQL    
	function_postgre(df)

		#print the time
	#elapsed_time = asyncio.get_event_loop().time() - start_time

	print("\n")
	#print(f"BS4 crawlers finished! all in: {elapsed_time:.2f} seconds.", "\n")