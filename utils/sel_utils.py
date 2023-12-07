import asyncio
import logging
from sql.clean_loc import clean_location_rows, convert_names_to_codes
from utils.handy import *




def clean_postgre_sel(df, csv_path, db):
	
	""" CLEANING AVOIDING DEPRECATION WARNING """
	for col in df.columns:
		if col == 'description':
			df[col] = df[col].str.replace(r'[{}[\]\'",]', '', regex=True)
		elif col == 'location':
			df[col] = df[col].str.replace(r'[{}[\]\'",]', '', regex=True)
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
	df.to_csv(csv_path, index=False)

	#Log it 
	logging.info('Finished Selenium_Crawlers. Results below ⬇︎')
	
	# SEND IT TO TO PostgreSQL    
	db(df)

def pipeline_json_postgre_sel(pipeline: str, PROD: str, TEST):
	if pipeline:
		if pipeline == 'MAIN':
			if PROD:
				JSON = PROD
				POSTGRESQL = to_postgre
				print("\n", f"Pipeline is set to 'MAIN'. Jobs will be sent to PostgreSQL's main_jobs table", "\n")
				# configure the logger
				LoggingMasterCrawler()
				return JSON, POSTGRESQL
		elif pipeline == 'FREELANCE':
			#TODO: Fix path
			JSON = '/selenium_resources/freelance.json'
			POSTGRESQL = freelance_postgre
			# configure the logger
			LoggingFreelanceCrawler()
			#print("\n", f"Reading {JSON}. Jobs will be sent to PostgreSQL's freelance table", "\n")
		elif pipeline == 'TEST':
			if TEST:
				JSON = TEST
				POSTGRESQL = test_postgre
				print("\n", f"Pipeline is set to 'TEST'. Jobs will be sent to PostgreSQL's test table", "\n")
				# configure the logger
				LoggingMasterCrawler()
				return JSON, POSTGRESQL
		else:
			print("\n", "Incorrect argument! Use 'MAIN', 'TEST' or 'FREELANCE' to run this script.", "\n")
