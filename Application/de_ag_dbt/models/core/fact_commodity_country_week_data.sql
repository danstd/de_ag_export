{{ config(materialized='table',
    cluster_by = ["countryName", "category"]) }}

select scd.*, cc.category, cc.categoryTotalMarker
from {{ ref('stg_commodity_data') }} scd
inner join {{ ref('commodity_categories') }} cc
on scd.commodityCode = cc.commodityCode