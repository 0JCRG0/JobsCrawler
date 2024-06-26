#!/usr/local/bin/python3

import json
import pretty_errors  # noqa: F401
import pandas as pd
import os
import psycopg2
import random
import logging
from datetime import date
from datetime import datetime
from dotenv import load_dotenv
from src.utils.handy import link_exists_in_db, USER_AGENTS
from src.utils.api_utils import clean_postgre_api, class_json_strategy
from src.utils.FollowLink import AsyncFollowLinkEchoJobs, async_follow_link
import asyncio
import aiohttp

""" LOAD THE ENVIRONMENT VARIABLES """

load_dotenv()

JSON_PROD = os.environ.get("JSON_PROD_API")
JSON_TEST = os.environ.get("JSON_TEST_API")
SAVE_PATH = os.environ.get("SAVE_PATH_API", "")
LOGGER_PATH = os.environ.get("LOGGER_PATH", "")

""" SET UP LOGGING FILE """
current_dir = os.path.dirname(os.path.abspath(__file__))

logging_file_path = os.path.join(current_dir, LOGGER_PATH)

setup_main_logger(logging_file_path)


async def async_api_template(pipeline):
    start_time = asyncio.get_event_loop().time()

    JSON = None
    POSTGRESQL = None

    if JSON_PROD and JSON_TEST:
        JSON, POSTGRESQL, URL_DB = test_or_prod(
            pipeline=pipeline, json_prod=JSON_PROD, json_test=JSON_TEST
        )

    if JSON is None or POSTGRESQL is None or URL_DB is None:
        logging.error(
            "Error: JSON and POSTGRESQL and URL_DB must be assigned valid values."
        )
        return

    logging.info("Async API crawler deployed!.")

    conn = psycopg2.connect(URL_DB)
    cur = conn.cursor()

    async def async_api_fetcher(session, api_obj):
        total_titles = []
        total_links = []
        total_descriptions = []
        total_pubdates = []
        total_locations = []
        total_timestamps = []
        rows = {
            "title": total_titles,
            "link": total_links,
            "description": total_descriptions,
            "pubdate": total_pubdates,
            "location": total_locations,
            "timestamp": total_timestamps,
        }

        name = api_obj["name"]
        api = api_obj["api"]
        elements_path = api_obj["elements_path"][0]
        location_default = elements_path["location_default"]
        class_json = api_obj["class_json"]
        follow_link = api_obj["follow_link"]
        inner_link_tag = api_obj["inner_link_tag"]
        random_user_agent = {"User-Agent": random.choice(USER_AGENTS)}

        async with aiohttp.ClientSession() as session:
            try:
                print("\n", f"Requesting {name}...")
                async with session.get(api, headers=random_user_agent) as response:
                    if response.status != 200:
                        print(
                            f"Received non-200 response ({response.status}) for API: {api}. Skipping..."
                        )
                        logging.warning(
                            f"Received non-200 response ({response.status}) for API: {api}. Skipping..."
                        )
                        pass
                    else:
                        try:
                            response_text = await response.text()
                            data = json.loads(response_text)
                            print(f"Successful request on {api}", "\n")
                            jobs = class_json_strategy(data, elements_path, class_json)
                            if jobs:
                                for job in jobs:
                                    job_data = {}

                                    job_data["title"] = job.get(
                                        elements_path["title_tag"], "NaN"
                                    )

                                    job_data["link"] = job.get(
                                        elements_path["link_tag"], "NaN"
                                    )

                                    """ WHETHER THE LINK IS IN THE DB """
                                    if await link_exists_in_db(
                                        link=job_data["link"],
                                        cur=cur,
                                        pipeline=pipeline,
                                    ):
                                        continue
                                    else:
                                        """WHETHER TO FOLLOW LINK"""
                                        if follow_link == "yes":
                                            default = job.get(
                                                elements_path["description_tag"], "NaN"
                                            )
                                            job_data["description"] = ""
                                            if name == "echojobs.io":
                                                job_data[
                                                    "description"
                                                ] = await AsyncFollowLinkEchoJobs(
                                                    session=session,
                                                    url_to_follow=job_data["link"],
                                                    selector=inner_link_tag,
                                                    default=default,
                                                )
                                            else:
                                                job_data["description"] = await async_follow_link(session=session, followed_link=job_data["link"], description_final=job_data["description"], inner_link_tag=inner_link_tag, default=default)  # type: ignore
                                        else:
                                            job_data["description"] = job.get(
                                                elements_path["description_tag"], "NaN"
                                            )

                                        today = date.today()
                                        job_data["pubdate"] = today

                                        job_data["location"] = (
                                            job.get(
                                                elements_path["location_tag"], "NaN"
                                            )
                                            or location_default
                                        )

                                        timestamp = datetime.now()
                                        job_data["timestamp"] = timestamp

                                        total_links.append(job_data["link"])
                                        total_titles.append(job_data["title"])
                                        total_pubdates.append(job_data["pubdate"])
                                        total_locations.append(job_data["location"])
                                        total_timestamps.append(job_data["timestamp"])
                                        total_descriptions.append(
                                            job_data["description"]
                                        )
                        except json.JSONDecodeError:
                            print(f"Failed to decode JSON for API: {api}. Skipping...")
                            logging.error(
                                f"Failed to decode JSON for API: {api}. Skipping..."
                            )
            except aiohttp.ClientError as e:
                print(f"Encountered a request error: {e}. Moving to the next API...")
                logging.error(
                    f"Encountered a request error: {e}. Moving to the next API..."
                )
                pass
        return rows

    async def gather_json_loads_api(session):
        with open(JSON) as f:
            data = json.load(f)
            apis = data[0]["apis"]

        tasks = [async_api_fetcher(session, api_obj) for api_obj in apis]
        results = await asyncio.gather(*tasks)

        combined_data = {
            "title": [],
            "link": [],
            "description": [],
            "pubdate": [],
            "location": [],
            "timestamp": [],
        }
        for result in results:
            for key in combined_data:
                combined_data[key].extend(result[key])

        title_len = len(combined_data["title"])
        link_len = len(combined_data["link"])
        description_len = len(combined_data["description"])
        pubdate_len = len(combined_data["pubdate"])
        location_len = len(combined_data["location"])
        timestamp_len = len(combined_data["timestamp"])

        lengths_info = """
			Titles: {}
			Links: {}
			Descriptions: {}
			Pubdates: {}
			Locations: {}
			Timestamps: {}
			""".format(
            title_len,
            link_len,
            description_len,
            pubdate_len,
            location_len,
            timestamp_len,
        )

        if (
            title_len
            == link_len
            == description_len
            == pubdate_len
            == location_len
            == timestamp_len
        ):
            logging.info("Async_API: LISTS HAVE THE SAME LENGHT. SENDING TO POSTGRE")
            clean_postgre_api(
                df=pd.DataFrame(combined_data),
                save_path=SAVE_PATH,
                function_postgre=POSTGRESQL,
            )
        else:
            logging.error(
                f"ERROR ON Async_API. LISTS DO NOT HAVE SAME LENGHT. FIX: \n {lengths_info}"
            )
            pass

    async with aiohttp.ClientSession() as session:
        await gather_json_loads_api(session)

    elapsed_time = asyncio.get_event_loop().time() - start_time
    print(f"Async APIs finished! all in: {elapsed_time:.2f} seconds.", "\n")
    logging.info(f"Async APIs finished! all in: {elapsed_time:.2f} seconds.")


async def main():
    await async_api_template("PROD")


if __name__ == "__main__":
    asyncio.run(main())
