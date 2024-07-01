import logging
from typing import Callable
import pandas as pd
from feedparser import FeedParserDict

async def async_get_feed_entries(feed: FeedParserDict):
	for entry in feed.entries:
		job_data = {}

		job_data["title"] = getattr(entry, title_tag) if hasattr(entry, loc_tag) else "NaN"

		job_data["link"] = getattr(entry, link_tag) if hasattr(entry, loc_tag) else "NaN"
		
		if await link_exists_in_db(link=job_data["link"], cur=cur, pipeline=pipeline):
			continue
		else:
			default = getattr(entry, description_tag) if hasattr(entry, loc_tag) else "NaN"
			if follow_link == 'yes':
				job_data["description"] = ""
				job_data["description"] = await async_follow_link(session=session, followed_link=job_data['link'], description_final=job_data["description"], inner_link_tag=inner_link_tag, default=default) # type: ignore
			else:
				job_data["description"] = default
			
			today = date.today()
			job_data["pubdate"] = today

			job_data["location"] = getattr(entry, loc_tag) if hasattr(entry, loc_tag) else "NaN"

			timestamp = datetime.now()
			job_data["timestamp"] = timestamp
			
			total_links.append(job_data["link"])
			total_titles.append(job_data["title"])
			total_pubdates.append(job_data["pubdate"])
			total_locations.append(job_data["location"])
			total_timestamps.append(job_data["timestamp"])
			total_descriptions.append(job_data["description"])

def clean_postgre_rss(df: pd.DataFrame, save_path: str, function_postgre: Callable):
	#Cleaning columns
	for col in df.columns:
		if col == 'description':
			if not df[col].empty:  # Check if the column has any rows
				df[col] = df[col].astype(str)  # Convert the entire column to string
				df[col] = df[col].str.replace(r'<.*?>|[{}[\]\'",]', '', regex=True) #Remove html tags & other characters
		elif col == 'location':
			if not df[col].empty:  # Check if the column has any rows
				df[col] = df[col].astype(str)  # Convert the entire column to string
				df[col] = df[col].str.replace(r'<.*?>|[{}[\]\'",]', '', regex=True) #Remove html tags & other characters
				#df[col] = df[col].str.replace(r'[{}[\]\'",]', '', regex=True)
				df[col] = df[col].str.replace(r'\b(\w+)\s+\1\b', r'\1', regex=True) # Removes repeated words
				df[col] = df[col].str.replace(r'\d{4}-\d{2}-\d{2}', '', regex=True)  # Remove dates in the format "YYYY-MM-DD"
				df[col] = df[col].str.replace(r'(USD|GBP)\d+-\d+/yr', '', regex=True)  # Remove USD\d+-\d+/yr or GBP\d+-\d+/yr.
				df[col] = df[col].str.replace('[-/]', ' ', regex=True)  # Remove -
				df[col] = df[col].str.replace(r'(?<=[a-z])(?=[A-Z])', ' ', regex=True)  # Insert space between lowercase and uppercase letters
				pattern = r'(?i)\bRemote Job\b|\bRemote Work\b|\bRemote Office\b|\bRemote Global\b|\bRemote with frequent travel\b'     # Define a regex patter for all outliers that use remote 
				df[col] = df[col].str.replace(pattern, 'Worldwide', regex=True)
				df[col] = df[col].replace('(?i)^remote$', 'Worldwide', regex=True) # Replace 
				df[col] = df[col].str.strip()  # Remove trailing white space
	
	#Save it in local machine
	df.to_csv(save_path, index=False)

	#Log it 
	logging.info('Finished RSS Reader. Results below ⬇︎')
	
	# SEND IT TO TO PostgreSQL    
	function_postgre(df)