# Gets reference data for usda export sales data.
import requests
import json
import os
from prefect import flow, task

class USDAReader():

    def __init__(self):
        USDA_API_KEY = os.getenv("USDA_API_KEY")
        self.headers = {"API_KEY": USDA_API_KEY,"Accept": "application/json"}

    def read(self, url: str, output_name: str):

        response = requests.get(url, headers=self.headers)
        if response.ok:
            response_txt = json.loads(response.text)
            with open(f"{output_name}.json", "w") as w:
                w.write(json.dumps(response_txt, indent=4)) 
        else:
            print(f"Bad response for {url}")
            raise ValueError

        return 0


# Retrieve reference data to local storage.
@task(log_prints=True, retries=3)
def reference_data_get():
    urls = {
        "commodities": "https://apps.fas.usda.gov/OpenData/api/esr/commodities",
        "units": "https://apps.fas.usda.gov/OpenData/api/esr/unitsOfMeasure",
        "regions": "https://apps.fas.usda.gov/OpenData/api/esr/regions",
        "countries": "https://apps.fas.usda.gov/OpenData/api/esr/countries"
        }

    usda_reader = USDAReader()
    for k, v in urls.items():
        usda_reader.read(url=v, output_name=k)

# retrieve most recent data release dates for commodities and market years
@task(log_prints=True, retries=3)
def data_date_get():
    drd = "https://apps.fas.usda.gov/OpenData/api/esr/datareleasedates"
    usda_reader = USDAReader()
    usda_reader.read(url=drd, output_name="data_release_dates")

@flow()
def usda_data_get():
    reference_data_get()
    data_date_get()



if __name__ == "__main__":
    usda_data_get()