# Gets reference data for usda export sales data.
import requests
import json
import os

USDA_API_KEY = os.getenv("USDA_API_KEY")

urls = {
    "commodities": "https://apps.fas.usda.gov/OpenData/api/esr/commodities",
    "units": "https://apps.fas.usda.gov/OpenData/api/esr/unitsOfMeasure",
    "regions": "https://apps.fas.usda.gov/OpenData/api/esr/regions",
    "countries": "https://apps.fas.usda.gov/OpenData/api/esr/countries",
    "data_release_dates": "https://apps.fas.usda.gov/OpenData/api/esr/datareleasedates"
       }

headers = {"API_KEY": USDA_API_KEY,
          "Accept": "application/json"}

json_dict = dict()

for k, v in urls.items():
    json_dict[k] = json.loads(requests.get(v, headers=headers).text)

for k, v in json_dict.items():
    with open(f"{k}.json", "w") as w:
        w.write(json.dumps(v, indent=4)) 