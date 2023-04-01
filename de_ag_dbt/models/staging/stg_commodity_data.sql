{{ config(materialized='view') }}

with commodity_data as 
(
  select *
  from {{ source('staging','external_commodity_data') }}
)

with commodity_ref as
(
select * from {{ source('staging', 'external_commodity_ref')}}
)

with country_ref as
(
select * from {{ source('staging', 'external_country_ref')}}
)

with region_ref as
(
select * from {{ source('staging', 'external_region_ref')}}
)

with unit_ref as
(
select * from {{ source('staging', 'external_unit_ref')}}
)


select
    -- identifiers
    {{ dbt_utils.surrogate_key(['commodityCode', 'countryCode', 'weekendingDate']) }} as commodityMarketID,
    commodity_data.commodityCode,
    coalesce(commodity_ref.commodityName, "No Commodity Name") as commodityName,
    commodity_data.countryCode,
    coalesce(country_ref.countryName, "No Country Name") as countryName,
    coalesce(region_ref.regionName, "No region Name") as regionName,
    commodity_data.weeklyExports,
    commodity_data.accumulatedExports,
    commodity_data.outstanding_sales,
    commodity_data.grossNewSales,
    commodity_data.currentMYNetSales,
    commodity_data.currentMYTotalCommitment,
    commodity_data.nextMYOutstandingSales,
    commodity_data.nextMYNetSales,
    commodity_data.unitId,
    commodity_data.weekendingDate,
    coalesce(unit_ref.unitName, "No Unit Name") as unitName,
from commodity_data
left join commodity_ref
    on commodity_data.commodityCode = commodity_ref.commodityCode
left join country_ref
    on commodity_data.countryCode = country_ref.countryCode
left join region_ref
    on country_ref.regionId = region_ref.regionId
left join unit_ref
    on commodity_data.unitId = unit_ref.unitId
    or (commodity_ref.unitId = unit_ref.unitId and commodity_data.unit_id is null);



-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
