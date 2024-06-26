import json
from psycopg2 import sql
import logging
import os
from dotenv import load_dotenv
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import pandas as pd
from psycopg2.extensions import cursor


""" LOAD THE ENVIRONMENT VARIABLES """

load_dotenv()

LOCAL_POSTGRE_URL = os.environ.get("LOCAL_POSTGRE_URL", "")
RENDER_POSTGRE_URL = os.environ.get("RENDER_POSTGRE_URL", "")
DATABASE_URL = os.environ.get("DATABASE_URL_DO", "")
MAIN_TABLE = "main_jobs"
TEST_TABLE = "test"

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



""" Loggers """

def setup_root_logger(file: str,) -> None:
    logging.basicConfig(
        filename=file,
        level=logging.DEBUG,
        force=True,
        filemode="a",
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def crawled_df_to_db(df: pd.DataFrame, cur: cursor | None, test: bool = False) -> None:

	if test:
		table = "test"
	table = "main_jobs"
	
	initial_count_query = f'''
		SELECT COUNT(*) FROM {table}
	'''
	if not cur:
		raise ValueError("Cursos cannot be None.")
	
	cur.execute(initial_count_query)
	initial_count_result = cur.fetchone()
	
	""" IF THERE IS A DUPLICATE LINK IT SKIPS THAT ROW & DOES NOT INSERTS IT
		IDs ARE ENSURED TO BE UNIQUE BCOS OF THE SERIAL ID THAT POSTGRE MANAGES AUTOMATICALLY
	"""
	jobs_added = []
	for _, row in df.iterrows():
		insert_query = f'''
			INSERT INTO {table} (title, link, description, pubdate, location, timestamp)
			VALUES (%s, %s, %s, %s, %s, %s)
			ON CONFLICT (link) DO NOTHING
			RETURNING *
		'''
		values = (row['title'], row['link'], row['description'], row['pubdate'], row['location'], row['timestamp'])
		cur.execute(insert_query, values)
		affected_rows = cur.rowcount
		if affected_rows > 0:
			jobs_added.append(cur.fetchone())


	""" LOGGING/PRINTING RESULTS"""

	final_count_query = f'''
		SELECT COUNT(*) FROM {table}
	'''
	cur.execute(final_count_query)
	final_count_result = cur.fetchone()

	if initial_count_result is not None:
		initial_count = initial_count_result[0]
	else:
		initial_count = 0
	jobs_added_count = len(jobs_added)
	if final_count_result is not None:
		final_count = final_count_result[0]
	else:
		final_count = 0

	postgre_report = {
		"Total count of jobs before crawling": initial_count,
		"Total number of unique jobs": jobs_added_count,
		"Current total count of jobs in PostgreSQL": final_count
		}

	logging.info(json.dumps(postgre_report))


async def link_exists_in_db(link: str, cur: cursor, test: bool = False) -> str | None:
	if test:
		table = "test"
	table = "main_jobs"

	query = sql.SQL(f"SELECT EXISTS(SELECT 1 FROM {table} WHERE link=%s)")
	cur.execute(query, (link,))

	result = cur.fetchone()

	return result[0] if result else None


""" OTHER UTILS """

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