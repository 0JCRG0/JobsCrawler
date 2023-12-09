import logging
from utils.handy import *
from typing import Callable
import pandas as pd



def clean_postgre_sel(df: pd.DataFrame, save_path: str, function_postgre: Callable):
	
	""" CLEANING AVOIDING DEPRECATION WARNING """
	for col in df.columns:
		if col == 'description' or col ==  'title':
			if not df[col].empty:  # Check if the column has any rows
				df[col] = df[col].astype(str)  # Convert the entire column to string
				df[col] = df[col].str.replace(r'<.*?>|[{}[\]\'",]', '', regex=True) #Remove html tags & other characters
		elif col == 'location':
			if not df[col].empty:  # Check if the column has any rows
				df[col] = df[col].astype(str)  # Convert the entire column to string
				df[col] = df[col].str.replace(r"\$\d{2,}K - \$\d{2,}K|\€\d{2,}K - \€\d{2,}K", '', regex=True) # Remove salaries
				df[col] = df[col].str.replace(r'<.*?>|[{}[\]\'",]', '', regex=True) #Remove html tags & other characters
				df[col] = df[col].str.replace(r'UTC[-+]\d{1,2} - UTC[-+]\d{1,2}', 'Worldwide', regex=True) #Remove stupid UTC
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
	logging.info('Finished Selenium_Crawlers. Results below ⬇︎')
	
	# SEND IT TO TO PostgreSQL    
	function_postgre(df)
