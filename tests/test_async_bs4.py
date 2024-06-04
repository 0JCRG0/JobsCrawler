from src.crawlers.async_bs4 import async_bs4_template
from dotenv import load_dotenv
import os
import asyncio
from src.utils.handy import setup_main_logger


""" LOAD THE ENVIRONMENT VARIABLES """
load_dotenv()

JSON_TEST = "resources/bs4_resources/bs4_test.json"
LOGGER_PATH = os.environ.get("LOGGER_PATH", "")


""" SET UP LOGGING FILE """
current_dir = os.path.dirname(os.path.abspath("bs4_test.json"))

print(current_dir)

exit()

logging_file_path = os.path.join(current_dir, LOGGER_PATH)

setup_main_logger(logging_file_path)


""" DATA """

JSON_TEST_PATH = os.path.join(current_dir, JSON_TEST)

async def main():
    await async_bs4_template(json_data_path=JSON_TEST_PATH, test=True)


if __name__ == "__main__":
    asyncio.run(main())