# Gets reference data for usda export sales data.
import requests
import json
import os
import pandas as pd
from pathlib import Path
from prefect import flow, task

class USDAReader():

    def __init__(self):
        USDA_API_KEY = os.getenv("USDA_API_KEY")
        self.headers = {"API_KEY": USDA_API_KEY,"Accept": "application/json"}

    def read(self, url: str, output_name: str, output_path: Path=None):

        if output_path is None:
            output_string = f"{output_name}.json"
        else:
            output_string = output_path / f"{output_name}.json"

        response = requests.get(url, headers=self.headers)
        if response.ok:
            response_txt = json.loads(response.text)
            with open(output_string, "w") as w:
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

# Reads new data release dates from local storage,
@task(log_prints=True, retries=3)
def data_release_date():
    drd = pd.read_json("data_release_dates.json")
    #date_cols=["marketYearStart", "marketYearEnd", "releaseTimeStamp"]
    used_cols=["commodityCode", "marketYear", "releaseTimeStamp"]
    #for col in date_cols:
    #    drd[col] = pd.to_datetime(drd[col], format="%Y-%m-%dT%H:%M:%S")
    drd["releaseTimeStamp"] = pd.to_datetime(drd["releaseTimeStamp"], format="%Y-%m-%dT%H:%M:%S")
    
    # Pull last release date for commodity and year from db for comparison.
    try:
        p_drd = pd.read_csv("previous_data_release_dates.csv",
           usecols=used_cols,
           parse_dates=["releaseTimeStamp"])
    except FileNotFoundError:
        return drd[used_cols]
    
    p_drd.rename(columns={"releaseTimeStamp": "previousReleaseTimeStamp"}, inplace=True)

    drd = drd.merge(right=p_drd, on=["commodityCode", "marketYear"], how="left")

    return drd.loc[drd["releaseTimeStamp"] != drd["previousReleaseTimestamp"], ["commodityCode", "marketYear"]]

# Main commodity data function.
@task(log_prints=True, retries=3)
def commodity_data_get(commodity_years: pd.DataFrame):
    DATA_PATH = Path(__file__).parent.resolve() / "data"
    usda_reader = USDAReader()
    for index, row in commodity_years.iterrows():
        cc = row["commodityCode"]
        year = row["marketYear"]
        url = f"https://apps.fas.usda.gov/OpenData/api/esr/exports/commodityCode/{cc}/allCountries/marketYear/{year}"
        usda_reader.read(url=url, output_name=f"{cc}_{year}", output_path=DATA_PATH)



# Get reference data and data release dates
@flow()
def usda_ref_data_get():
    reference_data_get()
    data_date_get()

# Get commodity data.
@flow()
def commodity_data():
    commodity_years = data_release_date()
    commodity_data_get(commodity_years=commodity_years)

if __name__ == "__main__":
    usda_ref_data_get()