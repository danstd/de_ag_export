# de_ag_export

This project is a pipeline for [U.S. Department of Agriculture (USDA) export commodity data](https://apps.fas.usda.gov/opendataweb/home).

Weekly export records are retrieved from the USDA API using Python with Prefect (de_ag_pipeline.py) and loaded to Google Cloud Storage as parquet files (converted through Pandas), where they are read into Google BigQuery.

Data transformations are done using DBT.

A dashboard has been set up using Looker Studio to visualize exports in metric tons:
[Dashboard Link](https://lookerstudio.google.com/reporting/e112d498-feb2-4705-abd0-5491deeda2fa)
 

- The pipeline requires Prefect Google Cloud Provider credential and Google Cloud Storage bucket blocks, as well as a DBT Core CLI Profile block. 

    - DBT block and deployments were created programatically, and are stored in the *Prefect Build Scripts* directory.

- DBT core installation is required

- All DBT files are stored in the *de_ag_dbt* directory.