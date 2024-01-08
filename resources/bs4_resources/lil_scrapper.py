import requests
from bs4 import BeautifulSoup
import urllib.request
import re
import sys
from selenium import webdriver
from bs4 import BeautifulSoup

#url_to_follow = "https://echojobs.io/job/wasabi-technologies-senior-storage-engineer-g88xl"
#selector = 'div[data-qa="btn-apply-bottom"] a'

#url_to_follow = "https://4dayweek.io/remote-jobs/fully-remote/anywhere?page=1"
url_to_follow = "https://ai-jobs.net/?reg=7"

container_selector = '.list-group.list-group-flush.mb-4'
title_selector = '.col.pt-2.pb-3 .h5.mb-2.text-body-emphasis'
location_selector = '.col.pt-2.pb-3 .float-end.text-end.text-body.d-inline-block.w-25.ms-2 span'
description_default = '.col.pt-2.pb-3 .badge.rounded-pill.text-bg-light'
link_selector = '.col.pt-2.pb-3'
inner_link = '.row.job-content-wrapper .col-sm-8.cols.hero-left'

def FollowLinkEchoJobs(url_to_follow: str) -> str:
	# Make a request to the website
	r = requests.get(url_to_follow)
	r.content

	# Use the 'html.parser' to parse the page
	soup = BeautifulSoup(r.content, 'html.parser')

	#print(soup.prettify())
	container = soup.select_one(container_selector)
	
	title = container.select(title_selector)
	location = container.select(location_selector)
	description = container.select(description_default)
	link = container.select(link_selector)
	# Find the 'div' tag with the class "job-detail mb-4"
	#div_tag = soup.find('div', {'class': selector})
	#description_tag = soup.select_one(inner_link)
	
	#description_final = description_tag.text
	print(title, location, description, link)

FollowLinkEchoJobs(url_to_follow)

exit(0)


s = "https:\\u002F\\u002Fjobs.lever.co\\u002Fwasabi\\u002F4c6f5c9b-0876-405e-9e8a-9c374b7295f7\\u002Fapply"
s = s.encode('unicode_escape').decode('unicode_escape')
print(s)

s = "https:\\u002F\\u002Fjobs.lever.co\\u002Fwasabi\\u002F4c6f5c9b-0876-405e-9e8a-9c374b7295f7\\u002Fapply"
s = codecs.decode(s, 'unicode_escape')
print(s)

