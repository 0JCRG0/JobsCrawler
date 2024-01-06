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
url_to_follow = "https://www.remoteimpact.io/?page=1"

container_selector = '.row.jobs-list'
title_selector = '.row.job-tile-title'
location_selector = '.job-tile-tags .remote-country'
description_default = '.job-tile-tags .tile-salary'
link = '.row.job-tile-title h3 a'
inner_link = '.row.job-content-wrapper .col-sm-8.cols.hero-left'

def FollowLinkEchoJobs(url_to_follow: str) -> str:
	#service = Service()
	#options = webdriver.ChromeOptions()
	
	driver = webdriver.Chrome(options=options, service=service)
	driver = webdriver.Chrome()
	#driver.implicitly_wait(4)
	driver.get(url_to_follow)

	html = driver.page_source
	
	soup = BeautifulSoup(html, 'lxml')

	print(soup.prettify())




	
	# Make a request to the website
	#r = requests.get(url_to_follow)
	#r.content

	# Use the 'html.parser' to parse the page
	#soup = BeautifulSoup(r.content, 'html.parser')

	#print(soup.prettify())
	#container = soup.select_one(".row.jobs-list")
	
	#title = container.select(selector)
	# Find the 'div' tag with the class "job-detail mb-4"
	#div_tag = soup.find('div', {'class': selector})
	#description_tag = soup.select_one(inner_link)
	
	#description_final = description_tag.text
	#print(description_final)

FollowLinkEchoJobs(url_to_follow)

exit(0)


s = "https:\\u002F\\u002Fjobs.lever.co\\u002Fwasabi\\u002F4c6f5c9b-0876-405e-9e8a-9c374b7295f7\\u002Fapply"
s = s.encode('unicode_escape').decode('unicode_escape')
print(s)

s = "https:\\u002F\\u002Fjobs.lever.co\\u002Fwasabi\\u002F4c6f5c9b-0876-405e-9e8a-9c374b7295f7\\u002Fapply"
s = codecs.decode(s, 'unicode_escape')
print(s)

