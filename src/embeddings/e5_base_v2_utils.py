import json
import timeit
from typing import Any

import numpy as np
import pandas as pd
import tiktoken
from dotenv import load_dotenv
from pgvector.psycopg2 import register_vector
from psycopg2.extensions import connection, cursor
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from torch import Tensor, no_grad
from torch.utils.data import DataLoader, Dataset
from tqdm import tqdm
from transformers.models.auto.modeling_auto import AutoModel
from transformers.models.auto.tokenization_auto import AutoTokenizer

from src.utils.logger_helper import get_custom_logger

load_dotenv()

PROD_TABLE = "embeddings_e5_base_v2"
TEST_TABLE = "test_embeddings_e5_base_v2"
CHUNK_SIZE = 15
MAX_LENGTH = 512
TOKENIZER = AutoTokenizer.from_pretrained("intfloat/e5-base-v2")
MODEL = AutoModel.from_pretrained("intfloat/e5-base-v2")

logger = get_custom_logger(__name__)

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
        print(
            f"Warning: Truncated string from {len(encoded_string)} tokens to {max_tokens} tokens."
        )
    return truncated_string

# THIS IS MADE FOR GPT MODELS I DO NOT THINK THAT IT TOKENIZES THE SAME AS FOR THE E5 MODEL
def num_tokens(text: str, model: str = "gpt-3.5-turbo") -> int:
    encoding = tiktoken.encoding_for_model(model)
    return len(encoding.encode(text))


def average_pool(last_hidden_states: Tensor, attention_mask: Any) -> Tensor:
    last_hidden = last_hidden_states.masked_fill(~attention_mask[..., None].bool(), 0.0)
    return last_hidden.sum(dim=1) / attention_mask.sum(dim=1)[..., None]


def e5_base_v2_query(query):
    query_e5_format = f"query: {query}"

    batch_dict = TOKENIZER(
        query_e5_format,
        max_length=MAX_LENGTH,
        padding=True,
        truncation=True,
        return_tensors="pt",
    )

    outputs = MODEL(**batch_dict)
    query_embedding = (
        average_pool(outputs.last_hidden_state, batch_dict["attention_mask"])
        .detach()
        .numpy()
        .flatten()
    )
    return query_embedding


def passage_e5_format(raw_descriptions: list) -> list:
    formatted_batches = [
        "passage: {}".format(raw_description) for raw_description in raw_descriptions
    ]
    return formatted_batches


def query_e5_format(raw_descriptions: list) -> list:
    formatted_batches = [
        "query: {}".format(raw_description) for raw_description in raw_descriptions
    ]
    return formatted_batches


@retry(
    stop=stop_after_attempt(7),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(Exception),
    before_sleep=before_sleep_log(logger, 1),
)
def to_embeddings_e5_base_v2(
    df: pd.DataFrame, cursor: cursor, conn: connection, test: bool
):
    table = PROD_TABLE

    if test:
        table = TEST_TABLE

    cursor.execute("CREATE EXTENSION IF NOT EXISTS vector")

    register_vector(conn)

    create_table_if_not_exist = f""" 
		CREATE TABLE IF NOT EXISTS {table} (
		id integer UNIQUE,
		job_info TEXT,
		timestamp TIMESTAMP,
		embedding vector(768)
		);"""

    cursor.execute(create_table_if_not_exist)

    initial_count_query = f"""
		SELECT COUNT(*) FROM {table}
	"""

    cursor.execute(initial_count_query)
    initial_count_result = cursor.fetchone()

    """ IF THERE IS A DUPLICATE ID IT SKIPS THAT ROW & DOES NOT INSERTS IT
		IDs UNIQUENESS SHOULD BE ENSURED DUE TO ABOVE.
	"""
    jobs_added = []
    for _, row in df.iterrows():
        insert_query = f"""
			INSERT INTO {table} (id, job_info, timestamp, embedding)
			VALUES (%s, %s, %s, %s)
			ON CONFLICT (id) DO NOTHING
			RETURNING *
		"""
        values = (row["id"], row["job_info"], row["timestamp"], row["embedding"])
        cursor.execute(insert_query, values)
        affected_rows = cursor.rowcount
        if affected_rows > 0:
            jobs_added.append(cursor.fetchone())

    """ logger/PRINTING RESULTS"""

    final_count_query = f"""
		SELECT COUNT(*) FROM {table}
	"""
    cursor.execute(final_count_query)
    final_count_result = cursor.fetchone()

    if initial_count_result:
        initial_count = initial_count_result[0]
    else:
        initial_count = 0
    jobs_added_count = len(jobs_added)

    if final_count_result:
        final_count = final_count_result[0]
    else:
        final_count = 0

    postgre_report_dict = {
        "Table": table,
        "Total count of jobs before crawling": initial_count,
        "Total number of unique jobs": jobs_added_count,
        "Current total count of jobs in PostgreSQL": final_count,
    }

    logger.info(f"{json.dumps(postgre_report_dict, indent=4)}")

    conn.commit()


def embeddings_e5_base_v2_to_df(
    batches_to_embed: list[str],
    jobs_info: list[str],
    batches_ids: list[str],
    batches_timestamps: list[str],
) -> pd.DataFrame:
    start_time = timeit.default_timer()

    def average_pool(last_hidden_states: Tensor, attention_mask: Tensor) -> Tensor:
        last_hidden = last_hidden_states.masked_fill(
            ~attention_mask[..., None].bool(), 0.0
        )
        return last_hidden.sum(dim=1) / attention_mask.sum(dim=1)[..., None]

    class TextDataset(Dataset):
        def __init__(self, texts, tokenizer):
            self.texts = texts
            self.tokenizer = tokenizer

        def __len__(self):
            return len(self.texts)

        def __getitem__(self, idx):
            return self.texts[idx]

    def collate_fn(batch, tokenizer):
        batch_dict = tokenizer(
            batch,
            max_length=MAX_LENGTH,
            padding=True,
            truncation=True,
            return_tensors="pt",
        )
        return batch_dict

    dataset = TextDataset(batches_to_embed, TOKENIZER)
    dataloader = DataLoader(
        dataset, batch_size=CHUNK_SIZE, collate_fn=lambda b: collate_fn(b, TOKENIZER)
    )

    embeddings_list = []

    with no_grad():
        for batch_dict in tqdm(dataloader, desc="Processing batches"):
            outputs = MODEL(**batch_dict)
            batch_embeddings = (
                average_pool(outputs.last_hidden_state, batch_dict["attention_mask"])
                .detach()
                .numpy()
            )

            embeddings_list.append(batch_embeddings)

    embeddings = np.vstack(embeddings_list)

    df_data = {
        "id": batches_ids,
        "job_info": jobs_info,
        "timestamp": batches_timestamps,
        "embedding": list(embeddings),
    }

    df = pd.DataFrame(df_data)

    elapsed_time = (timeit.default_timer() - start_time) / 60
    logger.info(
        f"Data was correctly embedded in {elapsed_time:.2f} minutes. Returning df for uploading to the db."
    )

    return df
