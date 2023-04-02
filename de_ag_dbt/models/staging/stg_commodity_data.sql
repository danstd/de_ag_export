{{ config(materialized='view') }}


select
    commodity_data.commodityCode,
    coalesce(commodity_ref.commodityName, 'No Commodity Name') as commodityName,
    commodity_data.countryCode,
    coalesce(country_ref.countryName, 'No Country Name') as countryName,
    region_ref.regionId,
    coalesce(region_ref.regionName, 'No region Name') as regionName,
    commodity_data.weeklyExports,
    commodity_data.accumulatedExports,
    commodity_data.outstandingSales,
    commodity_data.grossNewSales,
    commodity_data.currentMYNetSales,
    commodity_data.currentMYTotalCommitment,
    commodity_data.nextMYOutstandingSales,
    commodity_data.nextMYNetSales,
    commodity_data.weekendingDate,
    commodity_data.unitId,
    lower(coalesce(unit_ref.unitNames, 'No Unit Name')) as unitName,
from {{ source('staging', 'external_commodity_data')}} commodity_data
left join {{ source('staging', 'external_commodity_ref')}} commodity_ref
    on commodity_data.commodityCode = commodity_ref.commodityCode
left join {{ source('staging', 'external_country_ref')}} country_ref
    on commodity_data.countryCode = country_ref.countryCode
left join {{ source('staging', 'external_region_ref')}} region_ref
    on country_ref.regionId = region_ref.regionId
left join {{ source('staging', 'external_unit_ref')}} unit_ref
    on commodity_data.unitId = unit_ref.unitId
    or (commodity_ref.unitId = unit_ref.unitId and commodity_data.unitId is null)



-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
