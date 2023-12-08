import asyncio
import logging
from sql.clean_loc import clean_location_rows, convert_names_to_codes
from utils.handy import *



def clean_postgre_bs4(df: pd.DataFrame, S, Q):
	#df = pd.DataFrame(data) # type: ignore
	"""data_dic = dict(data) # type: ignore
	df = pd.DataFrame.from_dict(data_dic, orient='index')
	df = df.transpose()"""

		# count the number of duplicate rows
	num_duplicates = df.duplicated().sum()

			# print the number of duplicate rows
	print("Number of duplicate rows:", num_duplicates)

# remove duplicate rows based on all columns
	df = df.drop_duplicates()
		
		#CLEANING AVOIDING DEPRECATION WARNING
	for col in df.columns:
		if col == 'title' or col == 'description':
			if not df[col].empty: # Check if the column has any rows
				df[col] = df[col].apply(lambda x: str(x).replace(r'<[^<>]*>', '', regex=True) if isinstance(x, str) else x) #Remove html tags
				df[col] = df[col].apply(lambda x: str(x).replace(r'[{}[\]\'",]', '', regex=True) if isinstance(x, str) else x) #Remove shit
		elif col == 'location':
			if not df[col].empty: # Check if the column has any rows
				df[col] = df[col].apply(lambda x: str(x).replace(r'<[^<>]*>', '', regex=True) if isinstance(x, str) else x) #Remove html tags
				df[col] = df[col].apply(lambda x: str(x).replace(r'[{}[\]\'",]', '', regex=True) if isinstance(x, str) else x)
				df[col] = df[col].apply(lambda x: str(x).replace(r'\b(\w+)\s+\1\b', r'\1', regex=True) if isinstance(x, str) else x) # Removes repeated words
				df[col] = df[col].apply(lambda x: str(x).replace(r'\d{4}-\d{2}-\d{2}', '', regex=True) if isinstance(x, str) else x) # Remove dates in the format "YYYY-MM-DD"
				df[col] = df[col].apply(lambda x: str(x).replace(r'(USD|GBP)\d+-\d+/yr', '', regex=True) if isinstance(x, str) else x) # Remove USD\d+-\d+/yr or GBP\d+-\d+/yr.
				df[col] = df[col].apply(lambda x: str(x).replace('[-/]', ' ', regex=True) if isinstance(x, str) else x) # Remove -
				df[col] = df[col].apply(lambda x: str(x).replace(r'(?<=[a-z])(?=[A-Z])', ' ', regex=True) if isinstance(x, str) else x) # Insert space between lowercase and uppercase letters
				pattern = r'(?i)\bRemote Job\b|\bRemote Work\b|\bRemote Office\b|\bRemote Global\b|\bRemote with frequent travel\b'    # Define a regex patter for all outliers that use remote 
				df[col] = df[col].apply(lambda x: str(x).replace(pattern, 'Worldwide', regex=True) if isinstance(x, str) else x)
				df[col] = df[col].apply(lambda x: str(x).replace('(?i)^remote$', 'Worldwide', regex=True) if isinstance(x, str) else x) # Replace 
				df[col] = df[col].apply(lambda x: str(x).strip() if isinstance(x, str) else x) # Remove trailing white space

		
		#Save it in local machine
	df.to_csv(S, index=False)

		#Log it 
	logging.info('Finished bs4 crawlers. Results below ⬇︎')
		
		# SEND IT TO TO PostgreSQL    
	Q(df)

		#print the time
	#elapsed_time = asyncio.get_event_loop().time() - start_time

	print("\n")
	#print(f"BS4 crawlers finished! all in: {elapsed_time:.2f} seconds.", "\n")