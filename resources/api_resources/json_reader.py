import json
import requests
import pretty_errors

#Request the api...
url = 'https://api-career-vault-sstxk.ondigitalocean.app/v1/jobs/remote?page=1&search=&category=Software&role=&hideOnlyUS=1'

#response = requests.get(url)
headers = {"User-Agent": "my-app"}
#headers = {"User-Agent"}
response = requests.get(url, headers=headers)
response.raise_for_status()
if response.status_code == 200:
    data = json.loads(response.text)
    #data = response.json()

    pretty_json = json.dumps(data, indent=4)
    print(pretty_json, type(data))
else:
    print("aaaa")