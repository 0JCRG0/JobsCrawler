#!/Users/juanreyesgarcia/Dev/Python/Embeddings/JobsEmbeddings/env1/bin/python

import re
import tiktoken
import psycopg2
import pandas as pd
import logging
import timeit
import json
from datetime import datetime, timedelta
from torch import Tensor
from transformers import AutoTokenizer, AutoModel
import pyarrow.parquet as pq
from aiohttp import ClientSession
from pgvector.psycopg2 import register_vector
from dotenv import load_dotenv
import os

load_dotenv(".env")
LOGGER_MAIN = os.getenv("LOGGER_MAIN")
LOGGER_TEST = os.getenv("LOGGER_TEST")
SAVE_PATH = os.getenv("SAVE_PATH")
RENDER_POSTGRE_URL = os.environ.get("RENDER_POSTGRE_URL")
LOCAL_POSTGRE_URL = os.environ.get("LOCAL_POSTGRE_URL")
DATABASE_URL = os.environ.get("DATABASE_URL_DO")



def clean_rows(s):
	if not isinstance(s, str):
		print(f"{s} is not a string! Returning unmodified")
		return s
	s = re.sub(r'\(', '', s)
	s = re.sub(r'\)', '', s)
	s = re.sub(r"'", '', s)
	s = re.sub(r",", '', s)
	return s

"""
def openai_ef(OPENAI_API_KEY):
	openai_embedding = embedding_functions.OpenAIEmbeddingFunction(
					api_key=OPENAI_API_KEY,
					model_name="text-embedding-ada-002"
				)
	return openai_embedding"""

def truncated_string(
	string: str,
	model: str,
	max_tokens: int,
	print_warning: bool = False,
) -> str:
	"""Truncate a string to a maximum number of tokens."""
	encoding = tiktoken.encoding_for_model(model)
	encoded_string = encoding.encode(string)
	truncated_string = encoding.decode(encoded_string[:max_tokens])
	if print_warning and len(encoded_string) > max_tokens:
		print(f"Warning: Truncated string from {len(encoded_string)} tokens to {max_tokens} tokens.")
	return truncated_string

def num_tokens(text: str, model: str ="gpt-3.5-turbo") -> int:
	#Return the number of tokens in a string.
	encoding = tiktoken.encoding_for_model(model)
	return len(encoding.encode(text))

def original_specs_txt_file(content: str): 
	timestamp = datetime.now()
	with open(SAVE_PATH + 'specs.txt', 'a') as file:
		file.write(f"\nAt {timestamp}\n")
		file.write("RAW BATCHES SPECS: ------- \n")
		file.write(content)

def summary_specs_txt_file(total_cost: float, processed_time: float): 
	with open(SAVE_PATH + 'specs.txt', 'a') as file:
		file.write("\nSUMMARISED BATCHES SPECS: ---------- \n")
		file.write(f"Total Cost: ${total_cost:.2f} USD\n")
		file.write(f"Processed Time: {processed_time:.2f} seconds\n\n")

def save_df_to_csv(id, original, summary):
	df_raw_summarised_batches = pd.DataFrame({
		"id": id,
		"original": original,
		"summary": summary})

	df_raw_summarised_batches.to_csv(SAVE_PATH + "raw_summarised_batches.csv", index=False)

def count_words(text: str) -> int:
	# Remove leading and trailing whitespaces
	text = text.strip()

	# Split the text into words using whitespace as a delimiter
	words = text.split()

	# Return the count of words
	return len(words)

def df_to_parquet(data: pd.DataFrame, filename:str):
	df = pd.DataFrame(data)
	df.to_parquet(SAVE_PATH+ f"{filename}.parquet", engine='pyarrow')
	print(f"Saved embeddings to ../{filename}.parquet")

def append_parquet(new_df: pd.DataFrame, filename: str):
	# Load existing data
	df = pd.read_parquet(SAVE_PATH + f'{filename}.parquet')
	
	logging.info(f"Preexisting df: {df}")
	logging.info(f"df to append: {new_df}")

	df = pd.concat([df, new_df], ignore_index=True)
	df = df.drop_duplicates(subset='id', keep='last')

	# Write back to Parquet
	df.to_parquet(SAVE_PATH + f'{filename}.parquet', engine='pyarrow')
	logging.info(f"{filename}.parquet has been updated")

def average_pool(last_hidden_states: Tensor,
				attention_mask: Tensor) -> Tensor:
	last_hidden = last_hidden_states.masked_fill(~attention_mask[..., None].bool(), 0.0)
	return last_hidden.sum(dim=1) / attention_mask.sum(dim=1)[..., None]

def e5_base_v2_query(query):
	tokenizer = AutoTokenizer.from_pretrained('intfloat/e5-base-v2')
	model = AutoModel.from_pretrained('intfloat/e5-base-v2')

	query_e5_format = f"query: {query}"

	batch_dict = tokenizer(query_e5_format, max_length=512, padding=True, truncation=True, return_tensors='pt')

	outputs = model(**batch_dict)
	query_embedding = average_pool(outputs.last_hidden_state, batch_dict['attention_mask']).detach().numpy().flatten()
	return query_embedding

def filter_last_two_weeks(df:pd.DataFrame) -> pd.DataFrame:
	# Get the current date
	current_date = datetime.now().date()
	
	# Calculate the date two weeks ago from the current date
	two_weeks_ago = current_date - timedelta(days=14)
	
	# Filter the DataFrame to keep only rows with timestamps in the last two weeks
	filtered_df = df[df["timestamp"].dt.date >= two_weeks_ago]
	
	return filtered_df

def passage_e5_format(raw_descriptions:list) -> list:
	formatted_batches = ["passage: {}".format(raw_description) for raw_description in raw_descriptions]
	return formatted_batches

def query_e5_format(raw_descriptions:list) -> list:
	formatted_batches = ["query: {}".format(raw_description) for raw_description in raw_descriptions]
	return formatted_batches

def set_dataframe_display_options():
	# Call the function to set the desired display options
	pd.set_option('display.max_columns', None)  # Show all columns
	pd.set_option('display.max_rows', None)  # Show all rows
	pd.set_option('display.width', None)  # Disable column width restriction
	pd.set_option('display.expand_frame_repr', False)  # Disable wrapping to multiple lines
	pd.set_option('display.max_colwidth', None)  # Display full contents of each column

def filter_df_per_country(df: pd.DataFrame, user_desired_country:str) -> pd.DataFrame:
	# Load the JSON file into a Python dictionary
	with open(SAVE_PATH + 'continent_countries_with_capitals.json', 'r') as f:
		data = json.load(f)

	# Function to get country information
	def get_country_info(user_desired_country):
		values = []
		for continent, details in data.items():
			for country in details['Countries']:
				if country['country_name'] == user_desired_country:
					values.append(country['country_name'])
					values.append(country['country_code'])
					values.append(country['capital_english'])
					for subdivision in country['subdivisions']:
						values.append(subdivision['subdivisions_code'])
						values.append(subdivision['subdivisions_name'])
		return values

	# Get information for a specific country
	country_values = get_country_info(user_desired_country)

	# Convert 'location' column to lowercase
	df['location'] = df['location'].str.lower()

	# Convert all country values to lowercase
	country_values = [value.lower() for value in country_values]

	# Create a mask with all False
	mask = pd.Series(False, index=df.index)

	# Update the mask if 'location' column contains any of the country values
	for value in country_values:
		mask |= df['location'].str.contains(value, na=False)

	# Filter DataFrame
	filtered_df = df[mask]

	return filtered_df


def to_embeddings_e5_base_v2(pipeline: str, df: pd.DataFrame, db_url:str):

	table = None

	if pipeline == "TEST":
		table = "test_embeddings"
		db = "DO's test Postgre"
	elif pipeline == "PROD":
		table = "embeddings_e5_base_v2"
		db = "DO's prod Postgre"
	elif pipeline == "LocalProd":
		table = "embeddings_e5_base_v2"
		db = "Local's Postgre"
	else:
		logging.error("Incorrect argument! Use either 'PROD', ''LocalProd or 'TEST' to run this script.")
		raise

	try:
		# create a connection to the PostgreSQL database
		cnx = psycopg2.connect(db_url)

		logging.info(f"Pipeline selected = {pipeline}. Sending jobs to {db}")

		# create a cursor object
		cursor = cnx.cursor()
		cursor.execute('CREATE EXTENSION IF NOT EXISTS vector')

		#Register the vector type with your connection or cursor
		register_vector(cnx)

		create_table_if_not_exist = f""" 
			CREATE TABLE IF NOT EXISTS {table} (
			id integer UNIQUE,
			job_info TEXT,
			timestamp TIMESTAMP,
			embedding vector(768)
			);"""
		
		cursor.execute(create_table_if_not_exist)

		# execute the initial count query and retrieve the result
		initial_count_query = f"""
			SELECT COUNT(*) FROM {table}
		"""

		cursor.execute(initial_count_query)
		initial_count_result = cursor.fetchone()
		
		""" IF THERE IS A DUPLICATE ID IT SKIPS THAT ROW & DOES NOT INSERTS IT
			IDs UNIQUENESS SHOULD BE ENSURED DUE TO ABOVE.
		"""
		jobs_added = []
		for index, row in df.iterrows():
			insert_query = f"""
				INSERT INTO {table} (id, job_info, timestamp, embedding)
				VALUES (%s, %s, %s, %s)
				ON CONFLICT (id) DO NOTHING
				RETURNING *
			"""
			values = (row['id'], row['job_info'], row['timestamp'], row['embedding'])
			cursor.execute(insert_query, values)
			affected_rows = cursor.rowcount
			if affected_rows > 0:
				jobs_added.append(cursor.fetchone())


		""" LOGGING/PRINTING RESULTS"""

		final_count_query = f"""
			SELECT COUNT(*) FROM {table}
		"""
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
		print(f"{table} Table Report on {db}:", "\n")
		print(f"Total count of jobs before crawling: {initial_count}")
		print(f"Total number of unique jobs: {jobs_added_count}")
		print(f"Current total count of jobs in PostgreSQL: {final_count}")

		postgre_report = f"{table} Table Report on {db}:"\
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
	except Exception as e:
		logging.error(f"Exception at to_embeddings_e5_base_v2().\nException as follows: {e}.\n")
		raise Exception

def test_or_prod(
		pipeline: str,
		local_url_postgre: str = LOCAL_POSTGRE_URL,
		managed_url_postgre: str = DATABASE_URL,
		local_max_id_file: str = 'data\\max_id_local.txt',
		prod_max_id_file: str = 'data\\max_id_prod.txt',
		test_max_id_file: str = 'data\\max_id_test.txt'
		):
	
	if pipeline and local_url_postgre and managed_url_postgre and local_max_id_file and prod_max_id_file and test_max_id_file:
		if pipeline == 'PROD':
			logging.info(f"Pipeline is set to 'PROD'. Jobs will be sent to Render PostgreSQL's main_jobs table")
			return managed_url_postgre or "", prod_max_id_file or ""
		elif pipeline == 'TEST':
			logging.info(f"Pipeline is set to 'TEST'. Jobs will be sent to PostgreSQL's test table")
			return managed_url_postgre or "", test_max_id_file or ""
		elif pipeline == 'LocalProd':
			logging.info(f"Pipeline is set to 'LocalProd'. Jobs will be sent to Local PostgreSQL's main_jobs table")
			return local_url_postgre or "", local_max_id_file or ""
		else:
			print("\n", "Incorrect argument! Use either 'PROD', 'LocalProd' or 'TEST' to run this script.", "\n")
			logging.error("Incorrect arg for pipeline. Use either 'PROD', 'LocalProd' or 'TEST' to run this script.")
			assert pipeline=="PROD" or pipeline=="LocalProd" or pipeline=="TEST", "Incorrect arg for pipeline. Use either 'PROD', 'LocalProd' or 'TEST' to run this script."
	else:
		return None, None