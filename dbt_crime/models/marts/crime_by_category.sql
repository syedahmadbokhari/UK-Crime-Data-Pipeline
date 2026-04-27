-- Mart: monthly crime counts broken down by force and category.
-- Primary analytics table — drives the category breakdown chart.
with base as (
    select * from {{ ref('stg_crimes') }}
)

select
    year,
    month_num,
    month,
    force,
    crime_type,
    count(*)                                    as total_crimes,
    count(crime_id)                             as crimes_with_id,
    -- proportion of cases still open
    round(
        100.0 * count(*) filter (where last_outcome = 'Under investigation') / count(*),
        2
    )                                           as pct_under_investigation

from base
group by year, month_num, month, force, crime_type
order by year, month_num, force, total_crimes desc
