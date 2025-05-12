from src.embeddings.embed_latest_crawled_data import embed_data
from src.utils.logger_helper import get_custom_logger

logger = get_custom_logger(__name__)

embed_data(embedding_model="e5_base_v2", test=True)
