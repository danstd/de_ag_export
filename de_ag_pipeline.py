# Gets reference data for usda export sales data.
from datetime import timedelta
import requests
import json
import os
import pandas as pd
from pathlib import Path, PurePosixPath
from prefect import flow, task
from prefect_dbt.cli import DbtCliProfile, DbtCoreOperation
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
# from prefect_gcp.bigquery import BigQueryWarehouse
from prefect_gcp import GcpCredentials
import schema_to_pd_dtype


class USDAReader():

    def __init__(self):
        USDA_API_KEY = os.getenv("USDA_API_KEY")
        self.headers = {"API_KEY": USDA_API_KEY,"Accept": "application/json"}

    #def read(self, url: str, output_name: str, output_path: Path=None) -> json:
    def read(self, url: str) -> json:

        #if output_path is None:
        #    output_string = f"{output_name}.json"
        #else:
        #    output_string = output_path / f"{output_name}.json"

        response = requests.get(url, headers=self.headers)
        if not response.ok:
            print(f"Bad response for {url}")
            raise ValueError
        
        #response_txt = json.loads(response.text)
        #with open(output_string, "w") as w:
        #    w.write(json.dumps(response_txt, indent=4)) 
        response_json = json.loads(response.content)
        return response_json


# Write files from local storage to Google cloud bucket.
@task()
def gcs_data_write(from_path: Path, to_path: Path=None) -> None:
    gcs_block = GcsBucket.load("de-ag-block")
    gcs_block.upload_from_folder(from_folder=from_path, to_folder=to_path)
    return

# Retrieve reference data to local storage.
@task(log_prints=True, retries=3)
def reference_data_get(subdirectory: str="ref") -> list:
    urls = {
        "commodities": "https://apps.fas.usda.gov/OpenData/api/esr/commodities",
        "units": "https://apps.fas.usda.gov/OpenData/api/esr/unitsOfMeasure",
        "regions": "https://apps.fas.usda.gov/OpenData/api/esr/regions",
        "countries": "https://apps.fas.usda.gov/OpenData/api/esr/countries"
        }

    REF_PATH = Path(__file__).parent.resolve() / subdirectory
    usda_reader = USDAReader()
    for k, v in urls.items():
        json_response = usda_reader.read(url=v)

        ref = pd.DataFrame.from_records(json_response)
        schema_to_pd_dtype.df_dtype_set(df=ref, schema=k, file="source_schemas.json")
        ref.to_parquet(REF_PATH / f"{k}.parquet")
    return list(urls.keys())

# Retrieve most recent data release dates for commodities and market years
@task(log_prints=True, retries=3)
def data_date_get() -> None:
    drd = "https://apps.fas.usda.gov/OpenData/api/esr/datareleasedates"
    usda_reader = USDAReader()
    json_response = usda_reader.read(url=drd)
    with open("data_release_dates,json", "w") as w:
            w.write(json.dumps(json_response, indent=4)) 
    return

# Reads new data release dates from local storage,
@task(log_prints=True, retries=3)
def data_release_date() -> pd.DataFrame:
    drd = pd.read_json("data_release_dates.json")
    used_cols=["commodityCode", "marketYear", "releaseTimeStamp"]
    drd["releaseTimeStamp"] = pd.to_datetime(drd["releaseTimeStamp"], format="%Y-%m-%dT%H:%M:%S")
    
    # Pull last release date for commodity and year from db for comparison.
    try:
        p_drd = pd.read_csv("previous_data_release_dates.csv",
           usecols=used_cols,
           parse_dates=["releaseTimeStamp"])
    except FileNotFoundError:
        return drd[used_cols]
    
    p_drd.rename(columns={"releaseTimeStamp": "previousReleaseTimeStamp"}, inplace=True)
    drd = drd.merge(right=p_drd, on=["commodityCode", "marketYear"], how="outer")
    return drd

# Main commodity data function.
@task(log_prints=True, retries=3)
def commodity_data_get(commodity_years: pd.DataFrame) -> None:
    DATA_PATH = Path(__file__).parent.resolve() / "data"
    usda_reader = USDAReader()
    for index, row in commodity_years.iterrows():
        cc = row["commodityCode"]
        year = row["marketYear"]
        url = f"https://apps.fas.usda.gov/OpenData/api/esr/exports/commodityCode/{cc}/allCountries/marketYear/{year}"
        json_response = usda_reader.read(url=url)

        commodity = pd.DataFrame.from_records(json_response)
        schema_to_pd_dtype.df_dtype_set(df=commodity, schema="commodity_data", file="source_schemas.json")
        commodity.to_parquet(DATA_PATH / f"commodity_{cc}_{year}.parquet")
    return


# Get reference data and data release dates
@flow
def usda_ref_data_get(subdirectory: str="ref") -> None:
    data_date_get()
    refs = reference_data_get(subdirectory=subdirectory)
    from_path = Path(__file__).parent.resolve() / Path(subdirectory)
    #to_path = PurePosixPath("ref")
    gcs_data_write(from_path=from_path, to_path=None)

    return

# Get commodity data.
@flow
def commodity_data() -> None:
    commodity_years = data_release_date()
    commodity_data_get(commodity_years=commodity_years.loc[
        (~commodity_years["releaseTimeStamp"].isna()) &
        (commodity_years["releaseTimeStamp"] != commodity_years["previousReleaseTimeStamp"]),
        ["commodityCode", "marketYear"]]
        )
    from_path = Path(__file__).parent.resolve() / Path("data")
    #to_path = PurePosixPath("data")
    gcs_data_write(from_path=from_path, to_path=None)

    # Remove parquest files from Data path after successful write to google cloud.
    commodity_year_files = Path(from_path).glob("commodity_*.parquet")
    for file in commodity_year_files:
        os.remove(file)

    # Write commodity-year data to record retrieved commodity data.
    if "previousReleaseTimeStamp" in commodity_years.columns:
        commodity_years["releaseTimeStamp"] = commodity_years["releaseTimeStamp"].combine_first(commodity_years["previousReleaseTimeStamp"])
        commodity_years.drop(columns=["previousReleaseTimeStamp"], inplace=True)
    commodity_years.to_csv("previous_data_release_dates.csv", index=False)


# DBT flow
# https://prefecthq.github.io/prefect-dbt/
#@flow
#def trigger_dbt_flow() -> str:
#    DBT_PATH = Path(os.getcwd()) / "de_ag_dbt"
#    PROFILE_PATH = DBT_PATH / "profiles.yml"
#    result = DbtCoreOperation(
#        commands=["dbt run"],
#        project_dir=DBT_PATH,
#        profiles_dir=PROFILE_PATH,
#        overwrite_profiles=False
#    ).run()
#    return result

# DBT flow
# https://prefecthq.github.io/prefect-dbt/
def trigger_dbt_flow():
    DBT_PATH = Path(os.getcwd()) / "de_ag_dbt"
    PROFILE_PATH = DBT_PATH / "profiles.yml"
    dbt_cli_profile = DbtCliProfile.load("de-ag-dbt-profile-block")
    with DbtCoreOperation(
        commands=["dbt debug", "dbt run"],
        project_dir=DBT_PATH,
        profiles_dir=PROFILE_PATH,
        dbt_cli_profile=dbt_cli_profile,
    ) as dbt_operation:
        dbt_process = dbt_operation.trigger()
        # do other things before waiting for completion
        dbt_process.wait_for_completion()
        result = dbt_process.fetch_result()
    return result

# Main flow function.
@flow
def de_ag_flow() -> None:
    #usda_ref_data_get()
    #commodity_data()
    trigger_dbt_flow()     
    return 
    
if __name__ == "__main__":
    de_ag_flow()