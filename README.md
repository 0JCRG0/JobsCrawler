# Crawler-remote-jobs

*This project is not finished but it contains several crawlers which may be useful for anyone interested in finding a remote job without going to the hassle of actually doing all the research*

## IMPORTANT CONSIDERATIONS

1. Every crawler uses a different approach depending on the website. In the future I'd like to unify these scripts (this repo will be so much more powerful once it is all unified & fully automated). 

2. All the crawlers send the scraped data to a postgre database. So, you have to input your own credentials to make it work.

3. I have to write up a requirements.txt but this should not matter if you are using the most recent update of every library that is used here (if there are any changes I will write it down).

## Overview

### This repo has three main approaches in regards to data scraping.

1. If we are lucky the website/job board has an API which we can easily use to find all the elements we want from the JSON. 

2. If the website/job board does not have an exposed API then we can look at the Source Code and try to find its RSS feed. If this is found, we just include that url to remote-working-resources.csv and the script in ALL_RSS will do its magic with bs4. 

3. Finally, for the difficult but yet not impossible websites we use Selenium. Although these scripts are inherently slower, they are just incredibly powerful and versatile. (Note that this approach is only used for big websites which deserve a custom code, although if you know how I can unify these scripts I would be extremely helpful for the help/guidance)

