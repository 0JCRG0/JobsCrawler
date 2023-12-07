import re 
import psycopg2
from psycopg2 import sql
import logging
import json
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv


""" LOAD THE ENVIRONMENT VARIABLES """

load_dotenv()

user = os.environ.get('user')
password = os.environ.get('password')
host = os.environ.get('host')
port = os.environ.get('port')
database = os.environ.get('database')


""" LOAD THE ENVIRONMENT VARIABLES """

""" Loggers """
def LoggingMasterCrawler():
	# Define a custom format with bold text
	log_format = '%(asctime)s %(levelname)s: \n%(message)s\n'

	# Configure the logger with the custom format
	logging.basicConfig(filename="/Users/juanreyesgarcia/Library/CloudStorage/OneDrive-FundacionUniversidaddelasAmericasPuebla/DEVELOPER/PROJECTS/CRAWLER_ALL/logs/master_crawler.log",
						level=logging.INFO,
						format=log_format)

def LoggingFreelanceCrawler():
	# Define a custom format with bold text
	log_format = '%(asctime)s %(levelname)s: \n%(message)s\n'

	# Configure the logger with the custom format
	logging.basicConfig(filename="/Users/juanreyesgarcia/Library/CloudStorage/OneDrive-FundacionUniversidaddelasAmericasPuebla/DEVELOPER/PROJECTS/CRAWLER_ALL/logs/FreelanceCrawler.log",
						level=logging.INFO,
						format=log_format)

""" CLEANING FUNCTIONS"""
def clean_rows(s):
	if not isinstance(s, str):
		print(f"{s} is not a string! Returning unmodified")
		return s
	s = re.sub(r'{', '', s)
	s = re.sub(r'}', '', s)
	s = re.sub(r'[\[\]]', '', s)
	s = re.sub(r"'", '', s)
	s = re.sub(r",", ' ', s)
	return s

def cleansing_selenium_crawlers(s):
	if not isinstance(s, str):
		print(f"{s} is not a string! Returning unmodified")
		return s
	s = " ".join(s.split())
	s = re.sub(r'<[^>]+>', '', s)
	s = re.sub(r'n/', '', s)
	s = re.sub(r'"', '', s)
	s = re.sub(r'{', '', s)
	s = re.sub(r'}', '', s)
	s = re.sub(r'[\[\]]', '', s)
	s = re.sub(r"'", '', s)
	return s

def initial_clean(s):
	s = " ".join(s.split())
	s = re.sub(r'n/', '', s)
	return s

	# Handy cleansing function

def bye_regex(s):
	# Remove leading/trailing white space
	s = s.strip()
		# Replace multiple spaces with a single space
	s = re.sub(r'\s+', ' ', s)
		# Remove newline characters
	s = re.sub(r'\n', '', s)
		# Replace regex for í
	s = re.sub(r'√≠', 'í', s)
		# Replace word
	s = re.sub(r'Posted', '', s)
		# Remove HTML tags
	s = re.sub(r'<.*?>', '', s)

#CLEANING FUNCTIONS FOR RSS' crawlers
def clean_link_rss(s):
	# Remove leading/trailing white space
	s = s.strip()
		
	# Replace multiple spaces with a single space
	s = re.sub(r'\s+', ' ', s)
		
	# Remove newline characters
	s = re.sub(r'\n', '', s)
		
	return s

def clean_other_rss(s):
	if not isinstance(s, str):
		return s
	# Remove leading/trailing white space
	s = s.strip()
		
	# Replace multiple spaces with a single space
	s = re.sub(r'\s+', ' ', s)
		
	# Remove newline characters
	s = re.sub(r'\n', '', s)
		
	# Remove HTML tags
	s = re.sub(r'<.*?>', '', s)
		
	# Remove non-alphanumeric characters (except for spaces)
	s = re.sub(r'[^a-zA-Z0-9\s]+', '', s)
		
	# Remove symbols
	s = re.sub(r'[-–—•@Ôªø]+', '', s)
			
	return s

""" POSTGRE FUNCTIONS """


#TODO: Fix personal postgre
def personal_postgre(df):
	# create a connection to the PostgreSQL database
	cnx = psycopg2.connect(user='postgres', password='3312', host='localhost', port='5432', database='postgres')

	# create a cursor object
	cursor = cnx.cursor()

	# prepare the SQL query to create a new table
	create_table_query = '''
		CREATE TABLE IF NOT EXISTS personal (
			id VARCHAR(10),
			title VARCHAR(1000),
			link VARCHAR(1000) PRIMARY KEY,
			description VARCHAR(2000),
			pubdate TIMESTAMP,
			location VARCHAR(1000),
			UNIQUE (title, link)
		)
	'''

	# execute the create table query
	cursor.execute(create_table_query)

	# execute the initial count query and retrieve the result
	initial_count_query = '''
		SELECT COUNT(*) FROM personal
	'''
	cursor.execute(initial_count_query)
	initial_count_result = cursor.fetchone()

	# insert the DataFrame into the PostgreSQL database using an upsert strategy
	jobs_added = []
	for index, row in df.iterrows():
		insert_query = '''
			INSERT INTO personal (id, title, link, description, pubdate, location)
			VALUES (%s, %s, %s, %s, %s, %s)
			ON CONFLICT (title, link) DO UPDATE SET
				id = excluded.id,
				title = excluded.title,
				description = excluded.description,
				pubdate = excluded.pubdate,
				location = excluded.location
			RETURNING *
		'''
		values = (row['id'], row['title'], row['link'], row['description'], row['pubdate'], row['location'])
		cursor.execute(insert_query, values)
		if cursor.rowcount > 0:
			jobs_added.append(cursor.fetchone())

	final_count_query = '''
		SELECT COUNT(*) FROM personal
	'''
	# execute the count query and retrieve the result
	cursor.execute(final_count_query)
	final_count_result = cursor.fetchone()

	# calculate the number of unique jobs that were added
	if initial_count_result is not None:
		initial_count = initial_count_result[0]
	else:
		initial_count = 0
	jobs_added_count = len(jobs_added)
	if final_count_result is not None:
		final_count = final_count_result[0]
	else:
		final_count = 0
	unique_jobs = final_count - initial_count

	# check if the result set is not empty
	print("\n")
	print("PERSONAL DB RESULTS:", "\n")
	print(f"Total count of jobs before crawling: {initial_count}")
	print(f"Total number of jobs obtained by crawling: {jobs_added_count}")
	print(f"Total number of unique jobs added: {unique_jobs}")
	print(f"Current total count of jobs in PostgreSQL: {final_count}")

	# commit the changes to the database
	cnx.commit()

	# close the cursor and connection
	cursor.close()
	cnx.close()


def freelance_postgre(df):
	#TODO: Fix
	#call loggging
	LoggingFreelanceCrawler()

	# create a connection to the PostgreSQL database
	cnx = psycopg2.connect(user='postgres', password='3312', host='localhost', port='5432', database='postgres')

	# create a cursor object
	cursor = cnx.cursor()

	# prepare the SQL query to create a new table
	create_table_query = '''
		CREATE TABLE IF NOT EXISTS freelance (
			title VARCHAR(1000) PRIMARY KEY,
			link VARCHAR(1000),
			description VARCHAR(2000),
			pubdate TIMESTAMP,
			location VARCHAR(1000)
		)
	'''

	# execute the create table query
	cursor.execute(create_table_query)

	# execute the initial count query and retrieve the result
	initial_count_query = '''
		SELECT COUNT(*) FROM freelance
	'''
	cursor.execute(initial_count_query)
	initial_count_result = cursor.fetchone()

	# insert the DataFrame into the PostgreSQL database using an upsert strategy
	jobs_added = []
	for index, row in df.iterrows():
		insert_query = '''
			INSERT INTO freelance (title, link, description, pubdate, location)
			VALUES (%s, %s, %s, %s, %s)
			ON CONFLICT (title) DO UPDATE SET
				link = excluded.link,
				description = excluded.description,
				pubdate = excluded.pubdate,
				location = excluded.location
			RETURNING *
		'''
		values = (row['title'], row['link'], row['description'], row['pubdate'], row['location'])
		cursor.execute(insert_query, values)
		if cursor.rowcount > 0:
			jobs_added.append(cursor.fetchone())

	final_count_query = '''
		SELECT COUNT(*) FROM freelance
	'''
	# execute the count query and retrieve the result
	cursor.execute(final_count_query)
	final_count_result = cursor.fetchone()

	# calculate the number of unique jobs that were added
	if initial_count_result is not None:
		initial_count = initial_count_result[0]
	else:
		initial_count = 0
	jobs_added_count = len(jobs_added)
	if final_count_result is not None:
		final_count = final_count_result[0]
	else:
		final_count = 0
	unique_jobs = final_count - initial_count

	# check if the result set is not empty
	# check if the result set is not empty
	postgre_report = "FREELANCE TABLE REPORT:"\
					"\n"\
					f"Total count of jobs before crawling: {initial_count}" \
					"\n"\
					f"Total count of jobs found by crawling: {jobs_added_count}" \
					"\n"\
					f"Total count of unique jobs added: {unique_jobs}" \
					"\n"\
					f"Current total count of jobs in PostgreSQL: {final_count}"

	logging.info(postgre_report)
	
	print("\n")
	print("FREELANCE TABLE REPORT:", "\n")
	print(f"Total count of jobs before crawling: {initial_count}")
	print(f"Total number of jobs obtained by crawling: {jobs_added_count}")
	print(f"Total number of unique jobs added: {unique_jobs}")
	print(f"Current total count of jobs in PostgreSQL: {final_count}")

	# commit the changes to the database
	cnx.commit()

	# close the cursor and connection
	cursor.close()
	cnx.close()



def to_postgre(df):
	# create a connection to the PostgreSQL database
	cnx = psycopg2.connect(user='postgres', password='3312', host='localhost', port='5432', database='postgres')

	# create a cursor object
	cursor = cnx.cursor()

	# execute the initial count query and retrieve the result
	initial_count_query = '''
		SELECT COUNT(*) FROM main_jobs
	'''
	cursor.execute(initial_count_query)
	initial_count_result = cursor.fetchone()
	
	""" IF THERE IS A DUPLICATE LINK IT SKIPS THAT ROW & DOES NOT INSERTS IT
		IDs ARE ENSURED TO BE UNIQUE BCOS OF THE SERIAL ID THAT POSTGRE MANAGES AUTOMATICALLY
	"""
	jobs_added = []
	for index, row in df.iterrows():
		insert_query = '''
			INSERT INTO main_jobs (title, link, description, pubdate, location, timestamp)
			VALUES (%s, %s, %s, %s, %s, %s)
			ON CONFLICT (link) DO NOTHING
			RETURNING *
		'''
		values = (row['title'], row['link'], row['description'], row['pubdate'], row['location'], row['timestamp'])
		cursor.execute(insert_query, values)
		affected_rows = cursor.rowcount
		if affected_rows > 0:
			jobs_added.append(cursor.fetchone())


	""" LOGGING/PRINTING RESULTS"""

	final_count_query = '''
		SELECT COUNT(*) FROM main_jobs
	'''
	# execute the count query and retrieve the result
	cursor.execute(final_count_query)
	final_count_result = cursor.fetchone()

	# calculate the number of unique jobs that were added
	if initial_count_result is not None:
		initial_count = initial_count_result[0]
	else:
		initial_count = 0
	jobs_added_count = len(jobs_added)
	if final_count_result is not None:
		final_count = final_count_result[0]
	else:
		final_count = 0

	# check if the result set is not empty
	print("\n")
	print("MAIN_JOBS TABLE REPORT:", "\n")
	print(f"Total count of jobs before crawling: {initial_count}")
	print(f"Total number of unique jobs: {jobs_added_count}")
	print(f"Current total count of jobs in PostgreSQL: {final_count}")

	postgre_report = "MAIN_JOBS TABLE REPORT:"\
					"\n"\
					f"Total count of jobs before crawling: {initial_count}" \
					"\n"\
					f"Total number of unique jobs: {jobs_added_count}" \
					"\n"\
					f"Current total count of jobs in PostgreSQL: {final_count}"

	logging.info(postgre_report)

	# commit the changes to the database
	cnx.commit()

	# close the cursor and connection
	cursor.close()
	cnx.close()

def test_postgre(df):
	# create a connection to the PostgreSQL database
	cnx = psycopg2.connect(user='postgres', password='3312', host='localhost', port='5432', database='postgres')

	# create a cursor object
	cursor = cnx.cursor()

	# execute the initial count query and retrieve the result
	initial_count_query = '''
		SELECT COUNT(*) FROM test
	'''
	cursor.execute(initial_count_query)
	initial_count_result = cursor.fetchone()
	
	""" IF THERE IS A DUPLICATE LINK IT SKIPS THAT ROW & DOES NOT INSERTS IT
		IDs ARE ENSURED TO BE UNIQUE BCOS OF THE SERIAL ID THAT POSTGRE MANAGES AUTOMATICALLY
	"""
	jobs_added = []
	for index, row in df.iterrows():
		insert_query = '''
			INSERT INTO test (title, link, description, pubdate, location, timestamp)
			VALUES (%s, %s, %s, %s, %s, %s)
			ON CONFLICT (link) DO NOTHING
			RETURNING *
		'''
		values = (row['title'], row['link'], row['description'], row['pubdate'], row['location'], row['timestamp'])
		cursor.execute(insert_query, values)
		affected_rows = cursor.rowcount
		if affected_rows > 0:
			jobs_added.append(cursor.fetchone())


	""" LOGGING/PRINTING RESULTS"""

	final_count_query = '''
		SELECT COUNT(*) FROM test
	'''
	# execute the count query and retrieve the result
	cursor.execute(final_count_query)
	final_count_result = cursor.fetchone()

	# calculate the number of unique jobs that were added
	if initial_count_result is not None:
		initial_count = initial_count_result[0]
	else:
		initial_count = 0
	jobs_added_count = len(jobs_added)
	if final_count_result is not None:
		final_count = final_count_result[0]
	else:
		final_count = 0

	# check if the result set is not empty
	print("\n")
	print("TEST TABLE REPORT:", "\n")
	print(f"Total count of jobs before crawling: {initial_count}")
	print(f"Total number of unique jobs: {jobs_added_count}")
	print(f"Current total count of jobs in PostgreSQL: {final_count}")

	postgre_report = "TEST TABLE REPORT:"\
					"\n"\
					f"Total count of jobs before crawling: {initial_count}" \
					"\n"\
					f"Total number of unique jobs: {jobs_added_count}" \
					"\n"\
					f"Current total count of jobs in PostgreSQL: {final_count}"

	logging.info(postgre_report)
	# commit the changes to the database
	cnx.commit()

	# close the cursor and connection
	cursor.close()
	cnx.close()

async def link_exists_in_db(link, cur):

	query = sql.SQL("SELECT EXISTS(SELECT 1 FROM test WHERE link=%s)")
	cur.execute(query, (link,))

	# Fetch the result
	result = cur.fetchone()[0] # type: ignore

	return result


""" OTHER UTILS """

## Function to choose class_json_strategy
def class_json_strategy(data, elements_path, class_json):
	"""
	
	Given that some JSON requests are either
	dict or list we need to access the 1st dict if 
	needed
	
	"""
	if class_json == "dict":                    
		# Access the key of the dictionary, which is a list of job postings
		jobs = data[elements_path["dict_tag"]]
		return jobs
	elif class_json == "list":
		jobs = data
		return jobs

## Function to clean api_pubdates
def api_pubdate(s):
	if s is not None:
		pubdate = s[0:10]
		return pubdate

# FUNCTIONS FOR FORMATTING PUBDATES IN RSS
def adby_pubdate(s):
	#slice the jobs that are formatted as %a %d %b %Y
	if s is not None:
		if not s.startswith("20"):
			s_sliced = s[0:15]
			return s_sliced

def YMD_pubdate(s):
	#slice the jobs that are formatted as %a %d %b %Y
	if s is not None:
		YMD_pub = s[0:8]
		return YMD_pub

#Cleaning function indeed
def indeed_regex(s):
		pattern = r'<li>(.*?)<\/li>'
		matches = re.findall(pattern, s, re.DOTALL)
		if len(matches) > 0:
			text = matches[0]
			text = re.sub(r'<b>|<\/b>', '', text)
			return text
		else:
			return ''

def postgre_data(table_name:str = "main_jobs"):
	conn = psycopg2.connect(user='postgres', password='3312', host='localhost', port='5432', database='postgres')

	# Create a cursor object
	cur = conn.cursor()

	# Fetch new data from the table where id is greater than max_id
	cur.execute(f"SELECT * FROM {table_name}")
	new_data = cur.fetchall()


	# Close the database connection
	conn.commit()
	cur.close()
	conn.close()
	
	# Separate the columns into individual lists
	ids = [row[0] for row in new_data]
	titles = [row[1] for row in new_data]
	descriptions = [row[2] for row in new_data]
	locations = [row[3] for row in new_data]


	return ids, titles, locations, descriptions

def test_or_prod(pipeline: str, json_prod: str, json_test:str, postgre_prod, postgre_test):
	if pipeline and json_prod and json_test and postgre_prod and postgre_test:
		if pipeline == 'MAIN':
			print("\n", f"Pipeline is set to 'MAIN'. Jobs will be sent to PostgreSQL's main_jobs table", "\n")
			return json_prod or "", postgre_prod or ""
		elif pipeline == 'TEST':
			print("\n", f"Pipeline is set to 'TEST'. Jobs will be sent to PostgreSQL's test table", "\n")
			return json_test or "", postgre_test or ""
		else:
			print("\n", "Incorrect argument! Use either 'MAIN' or 'TEST' to run this script.", "\n")
			logging.error("Incorrect argument! Use either 'MAIN' or 'TEST' to run this script.")
			return None, None
	else:
		return None, None