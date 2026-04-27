-- Staging model: clean and type-cast the raw crimes table.
-- Handles nulls, strips whitespace, derives year/month columns.
with source as (
    select * from raw.crimes
),

cleaned as (
    select
        -- identity
        nullif(trim(crime_id), '')                          as crime_id,
        month,
        split_part(month, '-', 1)::integer                  as year,
        split_part(month, '-', 2)::integer                  as month_num,

        -- geography
        trim(force)                                         as force,
        trim(lsoa_code)                                     as lsoa_code,
        trim(lsoa_name)                                     as lsoa_name,
        -- extract district from LSOA name, e.g. "Bradford 001A" → "Bradford"
        regexp_replace(trim(lsoa_name), ' \d+.*$', '')      as district,
        longitude,
        latitude,
        trim(location)                                      as location,

        -- crime
        trim(crime_type)                                    as crime_type,
        coalesce(nullif(trim(last_outcome), ''), 'No outcome recorded')
                                                            as last_outcome,

        -- meta
        _loaded_at

    from source
    where month is not null
      and crime_type is not null
)

select * from cleaned
