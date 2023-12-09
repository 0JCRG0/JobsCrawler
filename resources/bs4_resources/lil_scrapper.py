import requests
from bs4 import BeautifulSoup
import re

#url_to_follow = "https://echojobs.io/job/wasabi-technologies-senior-storage-engineer-g88xl"
#selector = 'div[data-qa="btn-apply-bottom"] a'

url_to_follow = "https://app.remotesome.com/jobs/?page=0"

selector = 'job-detail mb-4'

def FollowLinkEchoJobs(url_to_follow: str, selector: str) -> str:

	
	# Make a request to the website
	r = requests.get(url_to_follow)
	r.content

	# Use the 'html.parser' to parse the page
	soup = BeautifulSoup(r.content, 'html.parser')

	print(soup.prettify())
	
	# Find the 'div' tag with the class "job-detail mb-4"
	#div_tag = soup.find('div', {'class': selector})

	# Get the text of the 'div' tag
	#text = div_tag.get_text()

	# Print the text
	#print(text)
	

FollowLinkEchoJobs(url_to_follow, selector)

exit(0)


s = "https:\\u002F\\u002Fjobs.lever.co\\u002Fwasabi\\u002F4c6f5c9b-0876-405e-9e8a-9c374b7295f7\\u002Fapply"
s = s.encode('unicode_escape').decode('unicode_escape')
print(s)

s = "https:\\u002F\\u002Fjobs.lever.co\\u002Fwasabi\\u002F4c6f5c9b-0876-405e-9e8a-9c374b7295f7\\u002Fapply"
s = codecs.decode(s, 'unicode_escape')
print(s)

