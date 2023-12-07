import asyncio
import logging
from sql.clean_loc import clean_location_rows, convert_names_to_codes
from utils.handy import *

def clean_postgre_api(df: pd.DataFrame, csv_path:str, db):

    """ Clean the rows accordingly """
    for col in df.columns:
        if col == 'location':
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
        if col == 'description':
            df[col] = df[col].astype(str).apply(clean_rows).apply(cleansing_selenium_crawlers)


    df.to_csv(csv_path, index=False)

    #Log
    logging.info('Finished API crawlers. Results below ⬇︎')

    ## PostgreSQL
    db(df)

