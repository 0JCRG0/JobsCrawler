# DUNNO IF I WILL USE IT AGAIN


def transform_data(input_data: dict) -> dict[str, dict[str, list[dict[str, list[str]]]]]:
	"""Simplify the data structure of 'all_locations.json' for better and easier mapping"""
	result = {}
	for continent, data in input_data.items():
		result[continent] = {"Countries": []}
		for country in data["Countries"]:
			country_name: str = country["country_name"].upper()
			country_code = country["country_code"].upper()
			if country["capital_english"] != "NaN":
				capital_english = country["capital_english"].upper()
			
			subdivisions = []
			if isinstance(country["subdivisions"], list):
				subdivisions = [sub["subdivisions_name"].upper() for sub in country["subdivisions"]]
			elif country["subdivisions"] != "NaN":
				subdivisions = [country["subdivisions"].upper()]
			
			transformed_country = {
				country_name: [country_code, capital_english] + subdivisions
			}
			result[continent]["Countries"].append(transformed_country)
	
	return result

#Example usage:

"""
input_file = "/root/JobsCrawler/src/notebooks/all_locations.json"
output_file = "/root/JobsCrawler/src/notebooks/all_locations_transformed.json"

input_data = load_json_file(input_file)
transformed_data = transform_data(input_data)
save_json_file(transformed_data, output_file)

print("Data transformation complete. Result saved to", output_file)
"""