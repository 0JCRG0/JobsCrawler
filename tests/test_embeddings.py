import sys
import os
from utils.logger_helper import get_custom_logger
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.embeddings.embed_latest_crawled_data import embed_data

logger = get_custom_logger(__name__)

embed_data(embedding_model="e5_base_v2", test=True)
