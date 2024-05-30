import psycopg2
from psycopg2 import sql
import logging
import os
from typing import Callable
from dotenv import load_dotenv
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import pandas as pd


""" LOAD THE ENVIRONMENT VARIABLES """

load_dotenv()

LOCAL_POSTGRE_URL = os.environ.get("LOCAL_POSTGRE_URL", "")
RENDER_POSTGRE_URL = os.environ.get("RENDER_POSTGRE_URL", "")
DATABASE_URL = os.environ.get("DATABASE_URL_DO", "")
LOGGER_PATH = os.environ.get("LOGGER_PATH", "")

USER_AGENTS = [
		'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
		'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
		'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:83.0) Gecko/20100101 Firefox/83.0',
		'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/17.17134',
		'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36',
		'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
		'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
		'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0',
		'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0',
		'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
		'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:15.0) Gecko/20100101 Firefox/15.0.1',
		'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',
		'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.79 Safari/537.36',
		'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
		'Mozilla/5.0 (X11; Linux x86_64; rv:10.0) Gecko/20100101 Firefox/10.0'
	]


""" LOAD THE ENVIRONMENT VARIABLES """

""" Loggers """
log_format = '%(asctime)s %(levelname)s: \n%(message)s\n'

logging.basicConfig(filename=LOGGER_PATH,
					level=logging.INFO,
					format=log_format)

""" POSTGRE FUNCTIONS """
def to_managed_prod_postgre(df: pd.DataFrame) -> None:
	cnx = psycopg2.connect(DATABASE_URL)

	cursor = cnx.cursor()

	initial_count_query = '''
		SELECT COUNT(*) FROM main_jobs
	'''
	cursor.execute(initial_count_query)
	initial_count_result = cursor.fetchone()
	
	""" IF THERE IS A DUPLICATE LINK IT SKIPS THAT ROW & DOES NOT INSERTS IT
		IDs ARE ENSURED TO BE UNIQUE BCOS OF THE SERIAL ID THAT POSTGRE MANAGES AUTOMATICALLY
	"""
	jobs_added = []
	for _, row in df.iterrows():
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
	cursor.execute(final_count_query)
	final_count_result = cursor.fetchone()

	if initial_count_result is not None:
		initial_count = initial_count_result[0]
	else:
		initial_count = 0
	jobs_added_count = len(jobs_added)
	if final_count_result is not None:
		final_count = final_count_result[0]
	else:
		final_count = 0

	print("\n")
	print("to_managed_prod_postgre() ON main_jobs report:", "\n")
	print(f"Total count of jobs before crawling: {initial_count}")
	print(f"Total number of unique jobs: {jobs_added_count}")
	print(f"Current total count of jobs in PostgreSQL: {final_count}")

	postgre_report = "to_managed_prod_postgre() ON main_jobs report:"\
					"\n"\
					f"Total count of jobs before crawling: {initial_count}" \
					"\n"\
					f"Total number of unique jobs: {jobs_added_count}" \
					"\n"\
					f"Current total count of jobs in PostgreSQL: {final_count}"

	logging.info(postgre_report)

	cnx.commit()

	cursor.close()
	cnx.close()

def to_managed_test_postgre(df: pd.DataFrame) -> None:
	cnx = psycopg2.connect(DATABASE_URL)

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
	print("to_managed_test_postgre() ON test report:", "\n")
	print(f"Total count of jobs before crawling: {initial_count}")
	print(f"Total number of unique jobs: {jobs_added_count}")
	print(f"Current total count of jobs in PostgreSQL: {final_count}")

	postgre_report = "to_managed_test_postgre() ON test report:"\
					"\n"\
					f"Total count of jobs before crawling: {initial_count}" \
					"\n"\
					f"Total number of unique jobs: {jobs_added_count}" \
					"\n"\
					f"Current total count of jobs in PostgreSQL: {final_count}"

	logging.info(postgre_report)

	cnx.commit()

	cursor.close()
	cnx.close()

def to_local_postgre(df: pd.DataFrame) -> None:
	cnx = psycopg2.connect(LOCAL_POSTGRE_URL)

	cursor = cnx.cursor()

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
	cursor.execute(final_count_query)
	final_count_result = cursor.fetchone()

	if initial_count_result is not None:
		initial_count = initial_count_result[0]
	else:
		initial_count = 0
	jobs_added_count = len(jobs_added)
	if final_count_result is not None:
		final_count = final_count_result[0]
	else:
		final_count = 0

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

def to_local_test(df: pd.DataFrame) -> None:
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

	result = cur.fetchone()[0] # type: ignore

	return result


""" OTHER UTILS """

def test_or_prod(
		pipeline: str,
		json_prod: str,
		json_test:str,
		to_managed_prod_postgre: Callable = to_managed_prod_postgre,
		to_managed_test_postgre: Callable = to_managed_test_postgre,
		local_postgre_prod: Callable = to_local_postgre,
		local_postgre_test: Callable = to_local_test,
		local_url_postgre: str = LOCAL_POSTGRE_URL,
		managed_postgre_url: str = DATABASE_URL) -> tuple[str, Callable, str]:
	
	if pipeline and json_prod and json_test and to_managed_prod_postgre and to_managed_test_postgre and local_postgre_prod and local_postgre_test and local_url_postgre and managed_postgre_url:
		if pipeline == 'PROD':
			logging.info("Pipeline is set to 'PROD'. Jobs will be sent to DO PostgreSQL. Expect jobs in main_jobs table")
			return json_prod, to_managed_prod_postgre, managed_postgre_url
		elif pipeline == 'TEST':
			logging.info("Pipeline is set to 'TEST'. Jobs will be sent to DO's PostgreSQL. Expect jobs in test table")
			return json_test, to_managed_test_postgre, managed_postgre_url
		elif pipeline == 'LocalProd':
			logging.info("Pipeline is set to 'LocalProd'. Jobs will be sent to Local PostgreSQL's main_jobs table")
			return json_prod, local_postgre_prod, local_url_postgre
		elif pipeline == 'LocalTest':
			logging.info("Pipeline is set to 'LocalProd'. Jobs will be sent to Local PostgreSQL's main_jobs table")
			return json_test, local_postgre_test, local_url_postgre
		else:
			error_msg = "Incorrect pipeline. Choose either 'PROD', 'TEST', 'LocalProd', 'LocalTest'"
			logging.error(error_msg)
			raise ValueError(error_msg)
	else:
		raise ValueError("One of the provided arguments is empty or None.")

def send_email(log_file_path):
	fromaddr = "maddy@rolehounds.com"
	toaddr = "juancarlosrg1999@gmail.com"
	msg = MIMEMultipart()

	msg['From'] = fromaddr
	msg['To'] = toaddr
	msg['Subject'] = "Finished crawling & embedding! logs attached"

	with open(log_file_path, 'rb') as f:
		part = MIMEBase('application', 'octet-stream')
		part.set_payload(f.read())
		encoders.encode_base64(part)
		part.add_header('Content-Disposition', 'attachment; filename="log.txt"')
		msg.attach(part)

	server = smtplib.SMTP('smtp-mail.outlook.com', 587)
	server.starttls()
	server.login(fromaddr, "buttercuP339!")
	server.send_message(msg)
	server.quit()

# At the end of your script
#send_email('C:\\Users\\juanc\\log_crawl.txt')