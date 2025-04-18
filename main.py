import pandas as pd
import requests
import json 
import numpy as np
import csv




import requests

url = "https://api.rentcast.io/v1/properties/random"

headers = {"accept": "application/json",
          "X-API-Key": "your_api_key_here",}

querystring ={"limit": "10000"}

response = requests.get(url, headers=headers, params = querystring)

data = response.json()

# print(response.json())

filename = "property.json"

with open(filename, 'w', encoding='utf-8') as file:
    json.dump(data, file, indent=4)



df = pd.read_json("property.json")

df['owner_type'] = df['owner'].apply(lambda x: x.get('type') if isinstance(x,dict) else x if isinstance(x,list) else None)