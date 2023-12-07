import re
import pandas as pd
import pretty_errors
import pycountry
from geonamescache import GeonamesCache
import json

    # Create an instance of the GeonamesCache class
gc = GeonamesCache()

    # Get a dictionary of all countries and their information, including capital cities
geo_countries = gc.get_countries()

df = pd.read_csv('/Users/juanreyesgarcia/Library/CloudStorage/OneDrive-FundacionUniversidaddelasAmericasPuebla/DEVELOPER/PROJECTS/CRAWLER_ALL/download/sql_distinct.csv')

    #df['location'] = df['location'].apply(clean_location)

def add_spaces(text):
    return re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', text)


def clean_location_rows(rows):
    if not isinstance(rows, str):
        #print(f"{rows} is not a string! Returning unmodified")
        return rows
        # Keep countries, country codes, states, remote words, and timezones
    # Remove double quotes
    rows = rows.replace('"', '')

    # Add spaces before uppercase letters
    rows = add_spaces(rows)

    keep_words = ['remote', 'remoto', 'international', 'anywhere', 'worldwide', 'utc', 'apac', 'nafta', 'latam', 'asean', 'mena', 'brics', 'anz', 'gcc', 'cee', 'nordic', 'europe', 'americas', 'asia', 'africa', 'oceania', 'antarctica', 'southeast asia', 'emea', 'uk', 'usa', 'united', 'states', 'north', 'america', 'kingdom', 'korea', 'global', 'south', 'latin', 'america']

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


df['location'] = df['location'].apply(clean_location_rows)

df.to_csv('/Users/juanreyesgarcia/Library/CloudStorage/OneDrive-FundacionUniversidaddelasAmericasPuebla/DEVELOPER/PROJECTS/CRAWLER_ALL/download/sql_distinct.csv', index=False)


SAVE_PATH = "/Users/juanreyesgarcia/Library/CloudStorage/OneDrive-FundacionUniversidaddelasAmericasPuebla/DEVELOPER/PROJECTS/CRAWLER_ALL/data"

def convert_names_to_codes(row):
    if isinstance(row, str):  # Check if row is a string

        # Country name transformed to their respective codes & clean
        with open(SAVE_PATH + '/continent_countries_with_capitals.json', 'r') as f:
            data = json.load(f)

        # Create dictionaries for country names and subdivision names
        country_dict = {}
        for continent in data.values():
            for country in continent['Countries']:
                country_dict[country['country_name']] = country['country_code']

        # Split the string into a list of names
        names = row.split()

        # Replace each name with its code
        codes = [country_dict.get(name, name) for name in names]

        # Remove duplicates and join the codes back into a string
        return ' '.join(pd.unique(codes))
    else:
        return row


# Apply the function to the 'location' column
#df['location'] = df['location'].apply(convert_names_to_codes)

#df.to_csv(SAVE_PATH + '/location_distinct.csv', index=False)



    #print(df['location'])

if __name__ == "__main__":
    clean_location_rows(df)