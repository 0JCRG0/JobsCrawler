import feedparser
import requests
import aiohttp
import asyncio
import json

#url = "https://api.cryptojobslist.com/jobs.rss"
url = "https://api.cryptojobslist.com/rss/remote.xml"

async def small_feed_parser(url_to_parse: str):
	async with aiohttp.ClientSession() as session:
		async with session.get(url_to_parse) as response:
			if response.status == 200:
				feed_data = await response.text()
				feed = feedparser.parse(feed_data)

				# Convert the feed data to a JSON string and print it
				#print(json.dumps(feed, indent=4))

				with open('data/feed.json', 'w') as outfile:
					json.dump(feed, outfile, indent=4)

if __name__ == "__main__":
	asyncio.run(small_feed_parser(url_to_parse=url))