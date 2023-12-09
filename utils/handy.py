import psycopg2
from psycopg2 import sql
import logging
import os
from typing import Callable
from dotenv import load_dotenv


""" LOAD THE ENVIRONMENT VARIABLES """

load_dotenv()

LOCAL_POSTGRE_URL = os.environ.get("LOCAL_POSTGRE_URL")
RENDER_POSTGRE_URL = os.environ.get("RENDER_POSTGRE_URL")



""" LOAD THE ENVIRONMENT VARIABLES """

""" Loggers """
def LoggingMasterCrawler():
	# Define a custom format with bold text
	log_format = '%(asctime)s %(levelname)s: \n%(message)s\n'

	# Configure the logger with the custom format
	logging.basicConfig(filename="/Users/juanreyesgarcia/Dev/Python/Crawlers/JobsCrawler/logs/LoggingMasterCrawler.log",
						level=logging.INFO,
						format=log_format)

""" POSTGRE FUNCTIONS """
def to_render_postgre(df):
	# create a connection to the PostgreSQL database
	cnx = psycopg2.connect(RENDER_POSTGRE_URL)

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
	print("to_render_postgre() ON main_jobs report:", "\n")
	print(f"Total count of jobs before crawling: {initial_count}")
	print(f"Total number of unique jobs: {jobs_added_count}")
	print(f"Current total count of jobs in PostgreSQL: {final_count}")

	postgre_report = "to_render_postgre() ON main_jobs report:"\
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

def to_local_postgre(df):
	# create a connection to the PostgreSQL database
	cnx = psycopg2.connect(LOCAL_POSTGRE_URL)

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

def to_local_test(df):
	# create a connection to the PostgreSQL database
	cnx = psycopg2.connect(LOCAL_POSTGRE_URL)

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

async def link_exists_in_db(link, cur, pipeline):
	
	table = None

	if pipeline == "PROD":
		table = "main_jobs"
	elif pipeline == "LocalProd":
		table = "main_jobs"
	else:
		table = "test"

	query = sql.SQL(f"SELECT EXISTS(SELECT 1 FROM {table} WHERE link=%s)")
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


def test_or_prod(
		pipeline: str,
		json_prod: str,
		json_test:str,
		render_postgre_prod: Callable = to_render_postgre,
		local_postgre_prod: Callable = to_local_postgre,
		local_postgre_test: Callable = to_local_test,
		local_url_postgre: str = LOCAL_POSTGRE_URL,
		render_url_postgre: str = RENDER_POSTGRE_URL,):
	
	if pipeline and json_prod and json_test and render_postgre_prod and local_postgre_prod and local_postgre_test and local_url_postgre and render_url_postgre:
		if pipeline == 'PROD':
			print("\n", f"Pipeline is set to 'PROD'. Jobs will be sent to Render PostgreSQL's main_jobs table", "\n")
			return json_prod or "", render_postgre_prod or "", render_url_postgre or ""
		elif pipeline == 'LocalProd':
			print("\n", f"Pipeline is set to 'LocalProd'. Jobs will be sent to Local PostgreSQL's main_jobs table", "\n")
			return json_prod or "", local_postgre_prod or "", local_url_postgre or ""
		elif pipeline == 'TEST':
			print("\n", f"Pipeline is set to 'TEST'. Jobs will be sent to PostgreSQL's test table", "\n")
			return json_test or "", local_postgre_test or "", local_url_postgre or ""
		else:
			print("\n", "Incorrect argument! Use either 'MAIN' or 'TEST' to run this script.", "\n")
			logging.error("Incorrect argument! Use either 'MAIN' or 'TEST' to run this script.")
			return None, None
	else:
		return None, None