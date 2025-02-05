from typing import Any
import aiohttp
from utils.logger_helper import get_custom_logger
import psycopg2
from psycopg2.extensions import cursor, connection
import asyncio
import json
import random
import pandas as pd
from constants import USER_AGENTS
from typing import TypedDict
import numpy as np
import os 
import re
from utils.logger_helper import get_custom_logger

logger = get_custom_logger(__name__)
 

DATA_DIR = os.path.join("src", "resources", "data")
LOCATIONS_DATA = os.path.abspath(os.path.join(DATA_DIR, "WorldLocations.json"))

############################# ADD LOCATION TAGS #############################

class Countries(TypedDict):
	country_name: str
	locations: list[str]

class WorldLocations(TypedDict):
	continent: str
	areas: list[str]
	countries: list[Countries]

def clean_and_split(s):
	tags = re.findall(r"'([^']*)'", s)
	return tags

def load_json_file(file_path: str):
	with open(file_path, 'r') as file:
		return json.load(file)

def save_json_file(data: dict, file_path: str) -> None:
	with open(file_path, 'w') as file:
		json.dump(data, file, indent=4)

def find_tag_in_location_data(word: str, location_data: WorldLocations) -> str:
	word_upper = word.upper()
	for continent, countries in location_data.items():
		
		if word_upper == continent.upper():
			return word_upper
		for zone in countries['Zones']: # type: ignore
			if word_upper == zone:
				return word_upper
		for country in countries['Countries']: # type: ignore
			for country_name, locations in country.items():
				if word_upper == country_name or word_upper in [loc for loc in locations]:
					return country_name
	return ""

def get_location_tags(df: pd.DataFrame, json_file_path: str) -> pd.DataFrame:
	"""
	Add location tags to a DataFrame using location data from a JSON file.

	Args:
		df (pd.DataFrame): Input DataFrame with a 'location' column.
		json_file_path (str): Path to JSON file with location data.

	Returns:
		pd.DataFrame: DataFrame with added 'location_tags' column.

	Processes each location entry, checking against JSON data. Combines adjacent 
	entries if needed to match locations in the JSON file.
	"""
	location_data = load_json_file(json_file_path)
	result = []
	i = 0
	while i < len(df):
		current_word = str(df.iloc[i]["location"])
		current_original_index = df.loc[i, "original_index"]
		
		tag = find_tag_in_location_data(current_word, location_data)
		
		if tag:
			result.append(tag)
			i += 1
		else:
			# If no match, try to concatenate with the next word if it has the same original_index
			if i + 1 < len(df) and df.loc[i + 1, "original_index"] == current_original_index:
				next_word = str(df.iloc[i + 1]['location'])

				compound_word = f"{current_word} {next_word}"

				tag = find_tag_in_location_data(compound_word, location_data)
				
				if tag:
					result.extend([tag, tag])
					i += 2
				else:
					result.append(np.nan)
					i += 1
			else:
				result.append(np.nan)
				i += 1

	df['location_tags'] = result
	return df


def add_location_tags_to_df(df: pd.DataFrame) -> pd.DataFrame:
	"""
	Add location tags to a DataFrame based on location data from a JSON file.

	Args:
		df (pd.DataFrame): Input DataFrame with a 'location' column.

	Returns:
		pd.DataFrame: DataFrame with added 'location_tags' column.

	Processes each location entry, checking against JSON data and combining adjacent
	entries as needed to match locations. Adds the identified location tags to the
	resulting DataFrame.
	"""
	original_df = df.copy()

	df['original_index'] = df.index

	df['location'] = df['location'].astype(str)

	df["location"] = df["location"].str.replace(",", "", regex=False).str.replace(")", "", regex=False).str.replace("(", "", regex=False).str.replace("|", " ", regex=False)

	df["location"] = df["location"].str.strip().str.split()
	
	df = df.explode("location").reset_index(drop=True)
	
	result_df = get_location_tags(df, LOCATIONS_DATA)

	result_df['location'] = result_df['location'].astype(str)

	result_df['location_tags'] = result_df['location_tags'].fillna('NaN')

	# Group by original_index and aggregate the locations and tags
	grouped_df = result_df.groupby('original_index').agg({
		'location': lambda x: ' '.join(x),
		'location_tags': lambda x: ''.join(str(x.unique()))
	})

	# Reset the index to make original_index a column again
	grouped_df = grouped_df.reset_index()

	grouped_df['location'] = grouped_df['location'].apply(lambda x: re.sub(r"[\[\]']", "", x))
	grouped_df['location_tags'] = grouped_df['location_tags'].apply(clean_and_split)

	# Sort by original_index to maintain the original order
	grouped_df = grouped_df.sort_values('original_index')

	grouped_df = grouped_df.drop('original_index', axis=1)

	grouped_df = grouped_df.reset_index(drop=True)

	original_df = original_df.drop('location', axis=1)
	
	final_df = pd.concat([original_df, grouped_df], axis=1)
	
	return final_df

############################# CLASS UTILS #############################


def crawled_df_to_db(df: pd.DataFrame, cur: cursor | None, test: bool = False) -> None:
	"""
	Insert a DataFrame of crawled job data into a PostgreSQL database.

	Args:
		df (pd.DataFrame): DataFrame containing the crawled job data.
		cur (psycopg2.extensions.cursor | None): Database cursor object.
		test (bool, optional): Flag to use 'test' table instead of 'main_jobs'. Defaults to False.

	Inserts the job data into the specified PostgreSQL table, handling duplicate entries.
	Logs the total count of jobs before and after the insertion, as well as the number
	of unique jobs added.
	"""
	table = "main_jobs"

	if test:
		table = "test"

	initial_count_query = f"""
		SELECT COUNT(*) FROM {table}
	"""
	if not cur:
		raise ValueError("Cursor cannot be None.")

	cur.execute(initial_count_query)
	initial_count_result = cur.fetchone()

	jobs_added = []
	for _, row in df.iterrows():
		insert_query = f"""
			INSERT INTO {table} (title, link, description, pubdate, location, timestamp, location_tags)
			VALUES (%s, %s, %s, %s, %s, %s, %s)
			ON CONFLICT (link) DO NOTHING
			RETURNING *
		"""
		values = (
			row["title"],
			row["link"],
			row["description"],
			row["pubdate"],
			row["location"],
			row["timestamp"],
			row["location_tags"],
		)
		cur.execute(insert_query, values)
		affected_rows = cur.rowcount
		if affected_rows > 0:
			jobs_added.append(cur.fetchone())

	final_count_query = f"""
		SELECT COUNT(*) FROM {table}
	"""
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
		"Table": table,
		"Total count of jobs before crawling": initial_count,
		"Total number of unique jobs": jobs_added_count,
		"Current total count of jobs in PostgreSQL": final_count,
	}

	logger.info(json.dumps(postgre_report))

############################# CRAWLER CLASS  #############################


class AsyncCrawlerEngine:
	"""
    Asynchronous web crawler engine for various data sources.

    This class is designed to handle different types of web crawling strategies (RSS, API, BS4)
    in an asynchronous manner. It's initialized with specific arguments for each strategy
    and is capable of fetching, processing, and storing data from multiple sources concurrently.

    The engine is used as part of a larger crawling system where multiple instances
    (one for each strategy) are run in parallel.

    Attributes:
        config: Configuration settings specific to the crawling strategy.
        test (bool): Flag to indicate whether the crawler is running in test mode.
        json_data_path (str): Path to the JSON file containing crawl configurations.
        custom_crawl_func: Custom function for crawling specific to the strategy.
        custom_clean_func: Custom function for cleaning and processing the crawled data.
        url_db (str): Database URL for storing the crawled data.
        conn (connection): Database connection object.
        cur (cursor): Database cursor object.

    Methods:
        run(): Executes the crawling process, including database setup, data fetching,
               processing, and storage. This method is called asynchronously from the main script.

    Usage:
        This class is instantiated in the main script for each crawling strategy (RSS, API, BS4).
        The instances are then run concurrently using asyncio.gather().

    Example:
        engine = AsyncCrawlerEngine(args)
		
        await engine.run()

    Note:
        The class relies on external configuration and custom functions passed through
        the arguments. It's designed to be flexible and accommodate different crawling
        strategies within the same asynchronous framework.
    """

	def __init__(self, args: Any) -> None:
		self.config = args.config
		self.test = args.test
		self.json_data_path = args.json_test_path if self.test else args.json_prod_path
		self.custom_crawl_func = args.custom_crawl_func
		self.custom_clean_func = args.custom_clean_func
		self.url_db = args.url_db
		self.conn: connection | None = None
		self.cur: cursor | None = None

	async def __load_configs(self) -> list[Any]:
		with open(self.json_data_path) as f:
			data = json.load(f)
		return [self.config(**url) for url in data]

	async def __fetch(
		self, session: aiohttp.ClientSession, config_instance: Any
	) -> str:
		random_user_agent = {"User-Agent": random.choice(USER_AGENTS)}
		async with session.get(
			config_instance.url, headers=random_user_agent
		) as response:
			if response.status != 200:
				logger.warning(
					f"Received non-200 response ({response.status}) requesting: {config_instance.url}. Skipping..."
				)
				pass
			logger.debug(f"random_header: {random_user_agent}")
			return await response.text()
	async def __gather_json_loads(self, session: aiohttp.ClientSession) -> None:
		configs = await self.__load_configs()

		tasks = [
			self.custom_crawl_func(
				lambda session, config=config: self.__fetch(session, config),
				session,
				config,
				self.cur,
				self.test,
			)
			for config in configs
		]
		results = await asyncio.gather(*tasks)

		combined_data = {
			key: []
			for key in [
				"title",
				"link",
				"description",
				"pubdate",
				"location",
				"timestamp",
			]
		}
		
		for result in results:
			for key in combined_data:
				combined_data[key].extend(result.get(key, []))

		lengths = {key: len(value) for key, value in combined_data.items()}
		
		if len(set(lengths.values())) == 1:
			df = self.custom_clean_func(pd.DataFrame(combined_data))
			final_df = add_location_tags_to_df(df)
			crawled_df_to_db(final_df, self.cur, self.test)
		else:
			logger.error(
				f"Error while calling {self.custom_crawl_func}. Data has uneven entries. "
				f"Exiting to avoid data corruption. Data lengths: {lengths}"
			)

	async def run(self) -> None:
		start_time = asyncio.get_event_loop().time()

		self.conn = psycopg2.connect(self.url_db)
		self.cur = self.conn.cursor()

		async with aiohttp.ClientSession() as session:
			await self.__gather_json_loads(session)

		self.conn.commit()
		self.cur.close()
		self.conn.close()

		elapsed_time = asyncio.get_event_loop().time() - start_time
		logger.info(f"Async BS4 crawlers finished! all in: {elapsed_time:.2f} seconds.")
