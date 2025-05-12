import json
import os
import re

import psycopg2
from dotenv import load_dotenv

from src.embeddings.e5_base_v2_utils import (
    embeddings_e5_base_v2_to_df,
    num_tokens,
    query_e5_format,
    to_embeddings_e5_base_v2,
    truncated_string,
)
from src.utils.logger_helper import get_custom_logger

logger = get_custom_logger(__name__)

load_dotenv(".env")
DB_URL = os.environ.get("URL_DB")
CONN = psycopg2.connect(DB_URL)
CURSOR = CONN.cursor()


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
    timestamp: str, test: bool = False
) -> tuple[list[str], list[str], list[str], list[str], list[str]]:
    table = "main_jobs"

    if test:
        table = "test"

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
                text, model="gpt-3.5-turbo", max_tokens=max_tokens, print_warning=True
            )
            batches.append(job_truncated)
            truncation_counter += 1

        total_tokens += num_tokens(text)

    if embedding_model == "e5_base_v2":
        approximate_cost = 0

    average_tokens_per_batch = round(total_tokens / len(batches), 2)
    batch_info = {
        "TOTAL NUMBER OF BATCHES": len(batches),
        "TOTAL NUMBER OF TOKENS": total_tokens,
        "MAX TOKENS PER BATCH": max_tokens,
        "NUMBER OF TRUNCATIONS": truncation_counter,
        "AVERAGE NUMBER OF TOKENS PER BATCH": average_tokens_per_batch,
        "APPROXIMATE COST OF EMBEDDING": f"${approximate_cost} USD",
    }

    logger.info(json.dumps(batch_info))

    if print_messages:
        for i, batch in enumerate(batches, start=1):
            print(f"Batch {i}:")
            print("".join(batch))
            print("Tokens per batch:", num_tokens(batch))
            print("\n")

        print(batch_info)

    return batches


def _get_max_timestamp(table: str = "last_embedding", test: bool = False) -> str:
    where_clause = "WHERE test = TRUE" if test else "WHERE test = FALSE"
    query = f"SELECT MAX(timestamp) FROM {table} {where_clause};"

    CURSOR.execute(query)

    result = CURSOR.fetchone()

    max_timestamp = result[0] if result else None

    if not max_timestamp:
        raise ValueError(f"The timestamp could not be found in {table}, where test is equal to {test}. No rows?")

    return max_timestamp


def _insert_max_timestamp(
    embedding_model: str, target_table: str = "last_embedding", test: bool = False
) -> None:
    source_table = f"embeddings_{embedding_model}"
    if test:
        source_table = f"test_embeddings_{embedding_model}"

    CURSOR.execute(
        f"SELECT id, timestamp FROM {source_table} ORDER BY timestamp DESC LIMIT 1;"
    )

    result = CURSOR.fetchone()

    if not result:
        raise ValueError(f"There are no entries in {source_table}")

    max_id, max_timestamp = result

    insert_query = f"""
        INSERT INTO {target_table} (id, timestamp, embedding_model, test)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE 
        SET timestamp = EXCLUDED.timestamp,
            embedding_model = EXCLUDED.embedding_model;
    """

    CURSOR.execute(insert_query, (max_id, max_timestamp, embedding_model, test))


def embed_data(embedding_model: str, test: bool = False) -> None:
    """
    Last Updated 25/09/24.
    
    Embed new job data using the specified embedding model.

    This function performs the following steps:
    1. Retrieves the timestamp of the last embedded data.
    2. Fetches new job entries from the database since the last timestamp.
    3. Cleans and formats the job data (title, location, description).
    4. Batches the formatted data, truncating if necessary.
    5. Generates embeddings using the specified model (currently only 'e5_base_v2').
    6. Stores the embeddings in the database.
    7. Updates the timestamp of the last embedded data.

    Args:
        embedding_model (str): The name of the embedding model to use (currently only 'e5_base_v2' is supported).
        test (bool, optional): If True, use test tables for database operations. Defaults to False.

    Raises
    ------
        ValueError: If no new rows are found or if an unsupported embedding model is specified.
        Exception: For any errors during the embedding process.

    Note:
        - Uses helper functions like _get_max_timestamp, _fetch_postgre_rows, _rows_to_nested_list, 
          _raw_descriptions_to_batches, and model-specific embedding functions.
        - Modifies the database, inserting new embeddings and updating the last embedded timestamp.
    """
    max_timestamp = _get_max_timestamp(test=test)

    ids, titles, locations, descriptions, timestamps = _fetch_postgre_rows(
        timestamp=max_timestamp, test=test
    )

    if not len(ids) > 1:
        error_msg = (
            f"No new rows. Obtained max_timestamp: {max_timestamp}. "
        )
        logger.error(error_msg)
        raise ValueError(error_msg)

    jobs_info = _rows_to_nested_list(titles, locations, descriptions)

    jobs_info_batches = _raw_descriptions_to_batches(jobs_info, embedding_model)

    if embedding_model == "e5_base_v2":
        try:
            e5_query_batches = query_e5_format(jobs_info_batches)

            df = embeddings_e5_base_v2_to_df(
                batches_to_embed=e5_query_batches,
                jobs_info=jobs_info_batches,
                batches_ids=ids,
                batches_timestamps=timestamps,
            )
            to_embeddings_e5_base_v2(df=df, cursor=CURSOR, conn=CONN, test=test)

            _insert_max_timestamp(embedding_model, test=test)
        except Exception as e:
            logger.error(e)
            raise e
    else:
        raise ValueError("The only supported embedding model is 'e5_base_v2'")

    CONN.commit()
    CURSOR.close()
    CONN.close()


if __name__ == "__main__":
    embed_data(embedding_model="e5_base_v2", test=False)
