{{ config(schema="Gold_layer", materialized="view", alias="Dimensions_Date") }}


with
    dates as (
        select day as date
        from unnest(generate_date_array(date('2010-01-01'), current_date())) as day
    )
select
    cast(format_date('%Y%m%d', date) as int64) as date_key,
    date,
    extract(year from date) as year,
    extract(quarter from date) as quarter,
    extract(month from date) as month,
    extract(day from date) as day,
    extract(isoweek from date) as iso_week,
    extract(dayofweek from date) as dow,
    (extract(dayofweek from date) in (1, 7)) as is_weekend
from dates
