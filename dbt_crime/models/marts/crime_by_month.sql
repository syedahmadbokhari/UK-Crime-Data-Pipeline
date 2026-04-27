-- Mart: total monthly crime counts per force — used for trend line charts.
with base as (
    select * from {{ ref('stg_crimes') }}
)

select
    year,
    month_num,
    month,
    force,
    count(*)                                        as total_crimes,
    -- YoY convenience columns
    lag(count(*)) over (
        partition by force, month_num
        order by year
    )                                               as prev_year_crimes,
    round(
        100.0 * (count(*) - lag(count(*)) over (
            partition by force, month_num order by year
        )) / nullif(lag(count(*)) over (
            partition by force, month_num order by year
        ), 0),
        2
    )                                               as yoy_pct_change

from base
group by year, month_num, month, force
order by force, year, month_num
