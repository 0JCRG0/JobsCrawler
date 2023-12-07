import re
import pandas as pd
import pretty_errors
import pycountry
from geonamescache import GeonamesCache
import json


def main_cleaning_location(df: pd.DataFrame):

    df['location'] = df['location'].str.replace(r'[{}[\]\'",]', '', regex=True) 
    df['location'] = df['location'].str.replace(r'\b(\w+)\s+\1\b', r'\1', regex=True) # Removes repeated words
    df['location'] = df['location'].str.replace(r'\d{4}-\d{2}-\d{2}', '', regex=True)  # Remove dates in the format "YYYY-MM-DD"
    df['location'] = df['location'].str.replace(r'(USD|GBP)\d+-\d+/yr', '', regex=True)  # Remove USD\d+-\d+/yr or GBP\d+-\d+/yr.
    df['location'] = df['location'].str.replace('[-/]', ' ', regex=True)  # Remove -
    df['location'] = df['location'].str.replace(r'(?<=[a-z])(?=[A-Z])', ' ', regex=True)  # Insert space between lowercase and uppercase letters
    pattern = r'(?i)\bRemote Job\b|\bRemote Work\b|\bRemote Office\b|\bRemote Global\b|\bRemote with frequent travel\b'     # Define a regex patter for all outliers that use remote 
    df['location'] = df['location'].str.replace(pattern, 'Worldwide', regex=True)
    df['location'] = df['location'].replace('(?i)^remote$', 'Worldwide', regex=True) # Replace 
    df['location'] = df['location'].str.strip()  # Remove trailing white space

    df.to_csv('/Users/juanreyesgarcia/Library/CloudStorage/OneDrive-FundacionUniversidaddelasAmericasPuebla/DEVELOPER/PROJECTS/CRAWLER_ALL/download/sql_distinct.csv', index=False)


df = pd.read_csv('/Users/juanreyesgarcia/Library/CloudStorage/OneDrive-FundacionUniversidaddelasAmericasPuebla/DEVELOPER/PROJECTS/CRAWLER_ALL/download/sql_distinct.csv')
df = main_cleaning_location(df=df)

"""

TO APPLY IN ALL THE CLEANING FUNCTIONS

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
"""