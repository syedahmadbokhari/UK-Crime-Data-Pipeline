-- Mart: LSOA-level crime hotspot aggregations for geospatial mapping.
-- Uses centroid lat/long per LSOA (average of all reported coordinates).
with base as (
    select * from {{ ref('stg_crimes') }}
    where latitude  is not null
      and longitude is not null
      and lsoa_code is not null
)

select
    lsoa_code,
    lsoa_name,
    district,
    force,
    year,
    month,
    -- centroid for the LSOA in this month
    round(avg(latitude),  6)    as centroid_lat,
    round(avg(longitude), 6)    as centroid_lon,
    count(*)                    as total_crimes,
    -- breakdown of top categories
    count(*) filter (where crime_type = 'Violence and sexual offences')    as violence_count,
    count(*) filter (where crime_type = 'Anti-social behaviour')           as asb_count,
    count(*) filter (where crime_type = 'Burglary')                        as burglary_count,
    count(*) filter (where crime_type = 'Vehicle crime')                   as vehicle_crime_count,
    count(*) filter (where crime_type = 'Shoplifting')                     as shoplifting_count,
    count(*) filter (where crime_type = 'Drugs')                           as drugs_count,
    -- hotspot tier: High / Medium / Low based on crime volume quantile
    case
        when count(*) >= 20 then 'High'
        when count(*) >= 10 then 'Medium'
        else 'Low'
    end                         as hotspot_tier

from base
group by lsoa_code, lsoa_name, district, force, year, month
order by total_crimes desc
