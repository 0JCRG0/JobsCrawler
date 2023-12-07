from geonamescache import GeonamesCache

# Create an instance of the GeonamesCache class
gc = GeonamesCache()

# Get a dictionary of all countries and their information, including capital cities
countries = gc.get_countries()

# Extract the capital cities from the countries dictionary
capital_cities = [countries[country]['capital'] for country in countries]

# Extract the capital cities from the countries dictionary
country_names = [countries[country]['name'] for country in countries]

print(country_names)