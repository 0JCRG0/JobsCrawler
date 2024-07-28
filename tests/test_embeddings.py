import sys
import os
import logging
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.embeddings.embed_latest_crawled_data import embed_data


LOGGER_PATH = os.path.join("logs", "main_logger.log")
(
    os.makedirs(os.path.dirname(LOGGER_PATH), exist_ok=True)
    if not os.path.exists(LOGGER_PATH)
    else None
)
open(LOGGER_PATH, "a").close() if not os.path.exists(LOGGER_PATH) else None


logging.basicConfig(
    filename=LOGGER_PATH,
    level=logging.INFO,
    force=True,
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
)

embed_data(embedding_model="e5_base_v2", test=True)
