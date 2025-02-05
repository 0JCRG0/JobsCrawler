# import requests
# from bs4 import BeautifulSoup
# import urllib.request
# import re
# import sys
# from selenium import webdriver
# import json
# from bs4 import BeautifulSoup

# #url_to_follow = "https://echojobs.io/job/wasabi-technologies-senior-storage-engineer-g88xl"
# #selector = 'div[data-qa="btn-apply-bottom"] a'

# #url_to_follow = "https://4dayweek.io/remote-jobs/fully-remote/anywhere?page=1"
# #url_to_follow = "https://www.occ.com.mx/empleos/de-home-office/trabajo-en-tecnologias-de-la-informacion-sistemas/?tm=14&page=1"

# url_to_follow ="https://bj.scjn.gob.mx/api/buscador/documento/tesis/T_dvMHYBN_4klb4Hg8R3"
# # You want to get the description and other 
# """container_selector = '.col-0-2-176.md5-0-2-285.jobCardContainer-0-2-536'
# title_selector = '.card-0-2-520.flat-0-2-522 .conFluid-0-2-61.gridContainer-0-2-568 .row-0-2-175.cardContent-0-2-562 .col-0-2-176.xs12-0-2-445.formatCol-0-2-569'
# location_selector = '.col.pt-2.pb-3 .float-end.text-end.text-body.d-inline-block.w-25.ms-2 span'
# description_default = '.adBodyDescription-0-2-564.spacing-0-2-565'
# link_selector = '.card-0-2-520.flat-0-2-522 a'
# inner_link = '.row.job-content-wrapper .col-sm-8.cols.hero-left' "card-0-2-520 flat-0-2-522 card-0-2-558 standout-0-2-559"
# "https://www.occ.com.mx/empleo/oferta/17988979/"
# """
# def FollowLinkEchoJobs(url_to_follow: str) -> str:
# 	# Make a request to the website
# 	r = requests.get(url_to_follow)
# 	r.content

# 	# Use the 'html.parser' to parse the page
# 	soup = BeautifulSoup(r.content, 'html.parser')

# 	data = str(soup)

# 	print(type(data))

# 	#parsed_json = json.loads(data)
# 	formatted_json = json.dumps(data, indent=4)

# 	#print(soup.prettify())
# 	print(formatted_json)

# FollowLinkEchoJobs(url_to_follow)

# exit(0)


# s = "https:\\u002F\\u002Fjobs.lever.co\\u002Fwasabi\\u002F4c6f5c9b-0876-405e-9e8a-9c374b7295f7\\u002Fapply"
# s = s.encode('unicode_escape').decode('unicode_escape')
# print(s)

# s = "https:\\u002F\\u002Fjobs.lever.co\\u002Fwasabi\\u002F4c6f5c9b-0876-405e-9e8a-9c374b7295f7\\u002Fapply"
# s = codecs.decode(s, 'unicode_escape')
# print(s)

