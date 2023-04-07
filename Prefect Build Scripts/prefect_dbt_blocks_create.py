from prefect_gcp.credentials import GcpCredentials
from prefect_dbt.cli import BigQueryTargetConfigs, DbtCliProfile, DbtCoreOperation


credentials = GcpCredentials.load("de-z-camp-gcp-credentials")
target_configs = BigQueryTargetConfigs(
    schema="de_ag",  # also known as dataset
    credentials=credentials,
)
target_configs.save("de-ag-dbt-target-block")

dbt_cli_profile = DbtCliProfile(
    name="de_ag_dbt",
    target="prod",
    target_configs=target_configs,
)
dbt_cli_profile.save("de-ag-dbt-profile-block")



dbt_cli_profile = DbtCliProfile.load("de-ag-dbt-profile-block")
dbt_core_operation = DbtCoreOperation(
    commands=["dbt run"],
    dbt_cli_profile=dbt_cli_profile,
    overwrite_profiles=True,
)
dbt_core_operation.save("de-ag-dbt-block")