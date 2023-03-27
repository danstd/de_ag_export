import json
import pandas as pd

# Read in schema information from a json file.
def schema_get(schema: str, file: str="source_schemas.json") -> list:
    with open("source_schemas.json", "r") as j:
        schema_dict = json.load(j)[schema]
    
    date_cols = list()
    for k, v in schema_dict.items():
        if v == "datetime":
            schema_dict[k] = "str"
            date_cols.append(k)
    
    return [schema_dict, date_cols]


# Using specified schema, create a template pandas dataframe.
def template_df_set(schema: str, file: str):
    schema_data = schema_get(schema=schema, file=file)
    template = pd.DataFrame(columns=list(schema_data[0].keys()))
    for col in template.columns:
        if col in schema_data[1]:
            template[col] = pd.to_datetime(template[col], format="%Y-%m-%dT%H:%M:%S")
        else:
            template[col] = template[col].astype(schema_data[0][col])
    return template

# Using specified schema, convert columns to the desired data types.
def df_dtype_set(df: pd.DataFrame, schema: str, file: str):
    schema_data = schema_get(schema=schema, file=file)
    for col in df.columns:
        if col in schema_data[1]:
            df[col] = pd.to_datetime(df[col], format="%Y-%m-%dT%H:%M:%S")
        else:
            df[col] = df[col].astype(schema_data[0][col])
    return