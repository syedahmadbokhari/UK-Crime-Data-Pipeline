-- Mart: aggregate performance metrics per force — used for force comparison.
with base as (
    select * from {{ ref('stg_crimes') }}
)

select
    force,
    year,
    month,
    count(*)                                            as total_crimes,
    count(distinct crime_type)                          as distinct_crime_types,
    round(
        100.0 * count(*) filter (where last_outcome = 'Under investigation') / count(*),
        2
    )                                                   as pct_under_investigation,
    round(
        100.0 * count(*) filter (where last_outcome = 'Investigation complete; no suspect identified') / count(*),
        2
    )                                                   as pct_no_suspect,
    round(
        100.0 * count(*) filter (where last_outcome not in (
            'Under investigation', 'No outcome recorded',
            'Investigation complete; no suspect identified'
        )) / count(*),
        2
    )                                                   as pct_resolved

from base
group by force, year, month
order by force, year, month
