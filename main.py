#!/usr/local/bin/python3

import logging
import timeit
import asyncio
import traceback
from async_rss import async_rss_template
from async_api import async_api_template
from async_bs4 import async_bs4_template
from async_sel import async_selenium_template
from async_indeed import async_indeed_template
from utils.handy import LoggingMasterCrawler

#SET UP LOGGING
LoggingMasterCrawler()

"""
In this code, the safe_call function is a wrapper that calls 
the provided function with the provided arguments and catches any 
exceptions that occur. It then returns the result 
(or the exception) along with the function name.
This way, when an exception occurs, you can log the 
function name along with the exception details.

"""

async def async_main(pipeline):
	# start master timer
	master_start_time = timeit.default_timer()

	#Start calling each crawler
	logging.info('ALL CRAWLERS DEPLOYED!')
	print("\n", "ALL CRAWLERS DEPLOYED!")

	async def safe_call(func, name, *args, **kwargs):
		try:
			return await func(*args, **kwargs), name
		except Exception as e:
			return e, name

	# Schedule tasks to run concurrently using asyncio.gather()
	results = await asyncio.gather(
		safe_call(async_api_template, 'async_api_template', pipeline),
		safe_call(async_rss_template, 'async_rss_template', pipeline),
		safe_call(async_bs4_template, 'async_bs4_template', pipeline),
		safe_call(async_selenium_template, 'async_selenium_template', pipeline),
		safe_call(async_indeed_template, 'async_indeed_template', "MX", "", pipeline)
	)

	for result, func_name in results:
		if isinstance(result, Exception):
			# handle exception
			logging.error(f"Exception occurred in function {func_name}: {type(result).__name__} in {result}\n{traceback.format_exc()}")
			continue

	#print the time
	elapsed_time = asyncio.get_event_loop().time() - master_start_time
	min_elapsed_time = elapsed_time / 60
	print(f"ALL ASYNC CRAWLERS FINISHED IN: {min_elapsed_time:.2f} minutes.", "\n")
	logging.info(f"ALL ASYNC CRAWLERS FINISHED IN: {min_elapsed_time:.2f} minutes.")

async def main():
	await async_main("TEST")

if __name__ == "__main__":
    asyncio.run(main())
