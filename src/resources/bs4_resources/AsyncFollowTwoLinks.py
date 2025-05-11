
import re

import aiohttp
import bs4

from src.utils.logger_helper import get_custom_logger

logger = get_custom_logger(__name__)


async def AsyncFollowLinkEchoJobs(session: aiohttp.ClientSession, url_to_follow: str, selector: str) -> str | None:

	async with session.get(url_to_follow) as r:
		try:
			if r.status == 200:
				
				logger.debug(f"""CONNECTION ESTABLISHED ON {url_to_follow}. USING FollowLinkEchoJobs()""", "\n")
				# Make a request to the website
				#r = requests.get(url_to_follow)
				request = await r.text()

				# Use the 'html.parser' to parse the page
				soup = bs4.BeautifulSoup(request, 'html.parser')

				# Find the 'a' tag under the 'div' with the 'data-qa' attribute of 'btn-apply-bottom'
				a_tag = soup.select_one(selector)
				logger.info(f"a_tag: {a_tag}")

				# Get the value of the 'href' attribute
				url = a_tag.get('href')

				# Use regular expression to remove "/apply" from the URL
				target_url = re.sub(r'/apply$', '', url)

				# Print the URL
				logger.info(f"target_url: {target_url}")
				# Send a GET request to the URL
				async with session.get(target_url) as r:
					try:
						if r.status == 200:
							#r = requests.get(modified_url)
							request = await r.text()

							# Parse the HTML content of the page with BeautifulSoup
							soup_2 = bs4.BeautifulSoup(request, 'html.parser')

							# Get the whole text of the page
							description_text = soup_2.get_text()

							if description_text:
								logger.info(f"SUCCESS on: {target_url}")
								return description_text
							else:
								description_final = 'NaN'
								return description_final
					except Exception as e:
						logger.info(f"Exception at AsyncFollowLinkEchoJobs() Following {target_url}\n {e}")
		except Exception as e:
			logger.info(f"Exception at AsyncFollowLinkEchoJobs(). Following {url_to_follow}\n {e}")