from typing import Any
import aiohttp
import logging
from collections.abc import Coroutine
import psycopg2
from psycopg2.extensions import cursor, connection
import asyncio
import json
import random
import pandas as pd
from src.constants import USER_AGENTS

# Set up named logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def crawled_df_to_db(df: pd.DataFrame, cur: cursor | None, test: bool = False) -> None:

    if test:
        table = "test"
    table = "main_jobs"

    initial_count_query = f"""
		SELECT COUNT(*) FROM {table}
	"""
    if not cur:
        raise ValueError("Cursos cannot be None.")

    cur.execute(initial_count_query)
    initial_count_result = cur.fetchone()

    """ IF THERE IS A DUPLICATE LINK IT SKIPS THAT ROW & DOES NOT INSERTS IT
		IDs ARE ENSURED TO BE UNIQUE BCOS OF THE SERIAL ID THAT POSTGRE MANAGES AUTOMATICALLY
	"""
    jobs_added = []
    for _, row in df.iterrows():
        insert_query = f"""
			INSERT INTO {table} (title, link, description, pubdate, location, timestamp)
			VALUES (%s, %s, %s, %s, %s, %s)
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
        )
        cur.execute(insert_query, values)
        affected_rows = cur.rowcount
        if affected_rows > 0:
            jobs_added.append(cur.fetchone())

    """ LOGGING/PRINTING RESULTS"""

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
        "Total count of jobs before crawling": initial_count,
        "Total number of unique jobs": jobs_added_count,
        "Current total count of jobs in PostgreSQL": final_count,
    }

    logging.info(json.dumps(postgre_report))


class AsyncCrawlerEngine:
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
                logging.warning(
                    f"Received non-200 response ({response.status}) for API: {self.config.url}. Skipping..."
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
                combined_data[key].extend(result[key])

        lengths = {key: len(value) for key, value in combined_data.items()}
        if len(set(lengths.values())) == 1:
            df = self.custom_clean_func(pd.DataFrame(combined_data))
            crawled_df_to_db(df, self.cur, self.test)
        else:
            logger.error(
                f"Error while calling {self.custom_crawl_func}. Data has uneven entries. Exiting to avoid data corruption. Data lengths: {lengths}"
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
