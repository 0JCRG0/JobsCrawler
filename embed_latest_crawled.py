import psycopg2
import os
from dotenv import load_dotenv
import pretty_errors  # noqa: F401
import logging
import re
from utils.e5_base_v2_utils import (
    query_e5_format,
    to_embeddings_e5_base_v2,
    num_tokens,
    truncated_string,
    embeddings_e5_base_v2_to_df,
)
import json
from utils.handy import setup_main_logger

load_dotenv(".env")
DB_URL = os.environ.get("DATABASE_URL_DO")
LOGGER_PATH = os.environ.get("LOGGER_PATH")
CONN = psycopg2.connect(DB_URL)
CURSOR = CONN.cursor()

setup_main_logger()

def _clean_rows(s):
    if not isinstance(s, str):
        print(f"{s} is not a string! Returning unmodified")
        return s
    s = re.sub(r"\(", "", s)
    s = re.sub(r"\)", "", s)
    s = re.sub(r"'", "", s)
    s = re.sub(r",", "", s)
    return s


def _fetch_postgre_rows(
    timestamp: str, table: str = "main_jobs"
) -> tuple[list[str], list[str], list[str], list[str], list[str]]:
    CURSOR.execute(
        f"SELECT id, title, description, location, timestamp FROM {table} WHERE timestamp > '{timestamp}'"
    )
    new_data = CURSOR.fetchall()

    ids = [row[0] for row in new_data]
    titles = [row[1] for row in new_data]
    descriptions = [row[2] for row in new_data]
    locations = [row[3] for row in new_data]
    timestamps = [row[4] for row in new_data]

    return ids, titles, locations, descriptions, timestamps


def _rows_to_nested_list(
    title_list: list[str], location_list: list[str], description_list: list[str]
) -> list[list[str]]:
    formatted_titles = ["<title> {} </title>".format(title) for title in title_list]
    cleaned_titles = [_clean_rows(title) for title in formatted_titles]
    formatted_locations = [
        "<location> {} </location>".format(location) for location in location_list
    ]
    cleaned_locations = [_clean_rows(location) for location in formatted_locations]
    formatted_descriptions = [
        "<description> {} </description>".format(description)
        for description in description_list
    ]
    cleaned_descriptions = [
        _clean_rows(description) for description in formatted_descriptions
    ]
    jobs_info = [
        [title, location, description]
        for title, location, description in zip(
            cleaned_titles, cleaned_locations, cleaned_descriptions
        )
    ]

    return jobs_info


def _raw_descriptions_to_batches(
    jobs_info: list[list[str]],
    embedding_model: str,
    max_tokens: int = 1000,
    print_messages: bool = False,
) -> list:
    batches = []
    total_tokens = 0
    truncation_counter = 0

    for i in jobs_info:
        text = " ".join(i)
        tokens_description = num_tokens(text)
        if tokens_description <= max_tokens:
            batches.append(text)
        else:
            job_truncated = truncated_string(
                text, model="gpt-3.5-turbo", max_tokens=max_tokens
            )
            batches.append(job_truncated)
            truncation_counter += 1

        total_tokens += num_tokens(text)

    if embedding_model == "e5_base_v2":
        approximate_cost = 0

    average_tokens_per_batch = total_tokens / len(batches)
    batch_info = {
        "TOTAL NUMBER OF BATCHES": len(batches),
        "TOTAL NUMBER OF TOKENS": total_tokens,
        "MAX TOKENS PER BATCH": max_tokens,
        "NUMBER OF TRUNCATIONS": truncation_counter,
        "AVERAGE NUMBER OF TOKENS PER BATCH": average_tokens_per_batch,
        "APPROXIMATE COST OF EMBEDDING": f"${approximate_cost} USD",
    }

    logging.info(json.dumps(batch_info))

    if print_messages:
        for i, batch in enumerate(batches, start=1):
            print(f"Batch {i}:")
            print("".join(batch))
            print("Tokens per batch:", num_tokens(batch))
            print("\n")

        print(batch_info)

    return batches


def _get_max_timestamp(table: str = "last_embedding") -> str:
    query = f"SELECT MAX(timestamp) FROM {table};"

    CURSOR.execute(query)

    result = CURSOR.fetchone()

    max_timestamp = result[0] if result else None

    if not max_timestamp:
        raise ValueError("The max timestamp must be present.")

    return max_timestamp

def _insert_max_timestamp(
    embedding_model: str,
    source_table: str = "embeddings_e5_base_v2",
    target_table: str = "last_embedding",
) -> None:
    CURSOR.execute(
        f"SELECT id, timestamp FROM {source_table} ORDER BY timestamp DESC LIMIT 1;"
    )

    result = CURSOR.fetchone()

    if not result:
        raise ValueError(f"There are no entries in {source_table}")

    max_id, max_timestamp = result

    insert_query = f"""
        INSERT INTO {target_table} (id, timestamp, embedding_model)
        VALUES (%s, %s, %s)
        ON CONFLICT (id) DO UPDATE 
        SET timestamp = EXCLUDED.timestamp,
            embedding_model = EXCLUDED.embedding_model;
    """

    CURSOR.execute(insert_query, (max_id, max_timestamp, embedding_model))


def embed_latest_crawled(embedding_model: str, test: bool = False):
    max_timestamp = _get_max_timestamp()

    ids, titles, locations, descriptions, timestamps = _fetch_postgre_rows(
        timestamp=max_timestamp
    )

    if not len(ids) > 1:
        error_msg = (
            "No new rows. Be sure crawler is populating main_jobs. Aborting execution."
        )
        logging.error(error_msg)
        raise ValueError(error_msg)

    jobs_info = _rows_to_nested_list(titles, locations, descriptions)

    jobs_info_batches = _raw_descriptions_to_batches(jobs_info, embedding_model)

    if embedding_model == "e5_base_v2":
        e5_query_batches = query_e5_format(jobs_info_batches)

        df = embeddings_e5_base_v2_to_df(
            batches_to_embed=e5_query_batches,
            jobs_info=jobs_info_batches,
            batches_ids=ids,
            batches_timestamps=timestamps,
        )
        to_embeddings_e5_base_v2(df=df, cursor=CURSOR, conn=CONN, test=test)

        if not test:
            _insert_max_timestamp(embedding_model)

    else:
        raise ValueError("The only supported embedding model is 'e5_base_v2'")

    CONN.commit()
    CURSOR.close()
    CONN.close()


if __name__ == "__main__":
    embed_latest_crawled(embedding_model="e5_base_v2", test=False)
