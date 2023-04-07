create or replace external table `de-z-camp.de_ag.external_commodity_data`
options (
  format = 'PARQUET',
  uris = ['gs://de_ag_export_bucket_0/commodity_*.parquet']
);

create or replace external table `de-z-camp.de_ag.external_commodity_ref`
options (
  format = 'PARQUET',
  uris = ['gs://de_ag_export_bucket_0/commodities.parquet']
);

create or replace external table `de-z-camp.de_ag.external_country_ref`
options (
  format = 'PARQUET',
  uris = ['gs://de_ag_export_bucket_0/countries.parquet']
);

create or replace external table `de-z-camp.de_ag.external_region_ref`
options (
  format = 'PARQUET',
  uris = ['gs://de_ag_export_bucket_0/regions.parquet']
);

create or replace external table `de-z-camp.de_ag.external_unit_ref`
options (
  format = 'PARQUET',
  uris = ['gs://de_ag_export_bucket_0/units.parquet']
);