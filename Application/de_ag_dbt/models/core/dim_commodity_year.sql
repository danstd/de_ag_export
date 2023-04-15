{{ config(materialized='table',
    cluster_by = ["category", "reportingYear"]) }}


 select 
    category,
    reportingYear,
    case when poundUnit is TRUE then 'metric tons' else unitName end as unitName,
    case when poundUnit is TRUE then grossNewSales * {{var('lb_to_mt', default=0.0004535924)}} else grossNewSales end as grossNewSales,
    case when poundUnit is TRUE then outstandingSales * {{var('lb_to_mt', default=0.0004535924)}} else outstandingSales end as outstandingSales,
    case when poundUnit is TRUE then weeklyExports * {{var('lb_to_mt', default=0.0004535924)}} else weeklyExports end as yearlyExports,
from (
    select 
        category,
        extract(YEAR from weekendingDate) as reportingYear,
        unitName,
        case when lower(unitName) like '%pound%' then TRUE else FALSE end as poundUnit,
        sum(grossNewSales) as grossNewSales,
        sum(outstandingSales) as outstandingSales,
        sum(weeklyExports) as weeklyExports,
    from {{ ref('fact_commodity_country_week_data') }} cwd
    where categoryTotalMarker is null
    and (unitName like '%pound%' or unitName like '%ton%')
    group by
        category,
        extract(YEAR from weekendingDate),
        unitName,
        case when lower(unitName) like '%pound%' then TRUE else FALSE end
    )