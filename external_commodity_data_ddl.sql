create or replace external table `de-z-camp.de_ag.external_commodity_data`
options (
  format = 'PARQUET',
  uris = ['gs://de_ag_export_bucket_0/commodity_*.parquet']
);