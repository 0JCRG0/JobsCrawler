#!/usr/local/bin/python3

import logging
import timeit
import asyncio
import traceback
from async_rss import async_rss_template
from async_api import async_api_template
from async_bs4 import async_bs4_template
#from async_sel import async_selenium_template
from embed_latest_crawled import embed_latest_crawled
#from async_indeed import async_indeed_template
#from utils.handy import send_email

#SET UP LOGGING
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

"""
In this script, safe_call() calls the provided functions 
with their respective arguments and catches any 
exceptions that occur. It then returns the result 
(or the exception) along with the function name.
This way, when an exception occurs, you can log the 
function name along with the exception details.
"""

async def async_main(pipeline):
	master_start_time = timeit.default_timer()

	logging.info('ALL CRAWLERS DEPLOYED!')

	async def safe_call(func, name, *args, **kwargs):
		try:
			return await func(*args, **kwargs), name
		except Exception as e:
			return e, name

	results = await asyncio.gather(
		safe_call(async_api_template, 'async_api_template', pipeline),
		safe_call(async_rss_template, 'async_rss_template', pipeline),
		safe_call(async_bs4_template, 'async_bs4_template', pipeline),
		#safe_call(async_selenium_template, 'async_selenium_template', pipeline),
		#safe_call(async_indeed_template, 'async_indeed_template', "MX", "", pipeline)
	)

	for result, func_name in results:
		if isinstance(result, Exception):
			logging.error(f"Exception occurred in function {func_name}: {type(result).__name__} in {result}\n{traceback.format_exc()}", exc_info=True)
			continue

	elapsed_time = asyncio.get_event_loop().time() - master_start_time
	min_elapsed_time = elapsed_time / 60
	print(f"ALL ASYNC CRAWLERS FINISHED IN: {min_elapsed_time:.2f} minutes.", "\n")
	logging.info(f"ALL ASYNC CRAWLERS FINISHED IN: {min_elapsed_time:.2f} minutes.")

async def main():
	await async_main("PROD")
	await asyncio.to_thread(embed_latest_crawled, embedding_model="e5_base_v2")

if __name__ == "__main__":
	asyncio.run(main())
