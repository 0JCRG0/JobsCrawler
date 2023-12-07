import re
import pandas as pd
import pycountry
from geonamescache import GeonamesCache
import psycopg2
import pretty_errors
from sqlalchemy import create_engine, text

# Your existing functions and variables here

def clean_all_locations():
    # Create an instance of the GeonamesCache class
    gc = GeonamesCache()

    # Get a dictionary of all countries and their information, including capital cities
    geo_countries = gc.get_countries()

    #df = pd.read_csv('/Users/juanreyesgarcia/Library/CloudStorage/OneDrive-FundacionUniversidaddelasAmericasPuebla/DEVELOPER/PROJECTS/CRAWLER_ALL/download/sql_distinct.csv')

    #df['location'] = df['location'].apply(clean_location)

    def add_spaces(text):
        return re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', text)


    def clean_location_rows(rows):
        if not isinstance(rows, str):
            print(f"{rows} is not a string! Returning unmodified")
            return rows
            # Keep countries, country codes, states, remote words, and timezones
        # Remove double quotes
        rows = rows.replace('"', '')

        # Add spaces before uppercase letters
        rows = add_spaces(rows)

        keep_words = ['remote', 'international', 'anywhere', 'worldwide', 'utc', 'apac', 'nafta', 'latam', 'asean', 'mena', 'brics', 'anz', 'gcc', 'cee', 'nordic', 'europe', 'americas', 'asia', 'africa', 'oceania', 'antarctica', 'southeast asia', 'emea', 'uk', 'usa', 'united', 'states', 'north', 'america', 'kingdom', 'korea', 'global', 'south', 'latin', 'america']

        # Get country codes and names
        country_codes = [country.alpha_2 for country in pycountry.countries]
        country_names = [country.name for country in pycountry.countries]
        # Extract the capital cities & countries from GeonamesCache
        capital_cities = [geo_countries[country]['capital'] for country in geo_countries]
        country_other_names = [geo_countries[country]['name'] for country in geo_countries]

        # US state codes
        state_codes = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']

        # Remove unwanted characters and split the text into words
        text = re.sub(r'[^a-zA-Z0-9\s\+\-]', ' ', rows)
        words = text.split()

        # Keep only the desired words
        cleaned_words = [word for word in words if any([word.lower().startswith(kw) for kw in keep_words]) or word in country_codes or word in country_names or word in state_codes or word in capital_cities or word in country_other_names or re.match(r'UTC[+\-]\d{1,2}', word)]

        # Join the cleaned words back into a string
        cleaned_text = ' '.join(cleaned_words)
        return cleaned_text

    #df['location'] = df['location'].apply(clean_location_rows)

    engine = create_engine("postgresql://postgres:3312@localhost:5432/postgres")
    with engine.connect() as conn:
        table_name = "main_jobs_copy"

        # Read the data from the PostgreSQL table in chunks

        chunksize = 500
        for chunk_df in pd.read_sql(text(f"SELECT * FROM {table_name}"), conn, chunksize=chunksize):
            # Apply the clean_location_rows function to the 'location' column
            chunk_df['location'] = chunk_df['location'].apply(clean_location_rows)

            # Update the PostgreSQL table with the cleaned data
            chunk_df.to_sql(table_name, conn, if_exists='append', index=False)

    # Close the database connection
if __name__ == "__main__":
    clean_all_locations()