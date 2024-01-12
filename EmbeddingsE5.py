
import torch.nn.functional as F
import torch
from torch import Tensor
from torch.utils.data import DataLoader, Dataset
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModel
import pretty_errors
import timeit
import logging
from datetime import datetime
import pandas as pd
import numpy as np


#@retry(wait=wait_random_exponential(min=1, max=60), stop=stop_after_attempt(6))
def embeddings_e5_base_v2_to_df(batches_to_embed: list[str], jobs_info: list[str], batches_ids: list[str], batches_timestamps: list[datetime]) -> pd.DataFrame:
    
    #Start the timer
    start_time = timeit.default_timer()

    CHUNK_SIZE = 15
    FORMATTED_E5_QUERY_BATCHES = batches_to_embed
    JOBS_INFO_BATCHES = jobs_info
    TIMESTAMPS = batches_timestamps
    IDS = batches_ids
    TOKENIZER = AutoTokenizer.from_pretrained("intfloat/e5-base-v2")
    MODEL = AutoModel.from_pretrained("intfloat/e5-base-v2")

    def average_pool(last_hidden_states: Tensor,
                    attention_mask: Tensor) -> Tensor:
        last_hidden = last_hidden_states.masked_fill(~attention_mask[..., None].bool(), 0.0)
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
        batch_dict = tokenizer(batch, max_length=512, padding=True, truncation=True, return_tensors='pt')
        return batch_dict

    # Create a dataset and a dataloader
    dataset = TextDataset(FORMATTED_E5_QUERY_BATCHES, TOKENIZER)
    dataloader = DataLoader(dataset, batch_size=CHUNK_SIZE, collate_fn=lambda b: collate_fn(b, TOKENIZER))

    # Process the data using the dataloader
    embeddings_list = []

    with torch.no_grad():
        for batch_dict in tqdm(dataloader, desc="Processing batches"):
            # Get embeddings
            outputs = MODEL(**batch_dict)
            batch_embeddings = average_pool(outputs.last_hidden_state, batch_dict['attention_mask']).detach().numpy()

            # Add batch embeddings to the list
            embeddings_list.append(batch_embeddings)

    # Concatenate embeddings and save to a single Parquet file
    EMBEDDINGS = np.vstack(embeddings_list)


    df_data = {
        'id': IDS,
        'job_info': JOBS_INFO_BATCHES,
        'timestamp': TIMESTAMPS,
        'embedding': list(EMBEDDINGS)
        }

    df = pd.DataFrame(df_data)

    elapsed_time = (timeit.default_timer() - start_time) / 60
    logging.info(f"\nembeddings_e5_base_v2_to_df() done! Elapsed time: {elapsed_time:.2f} minutes.")

    return df





