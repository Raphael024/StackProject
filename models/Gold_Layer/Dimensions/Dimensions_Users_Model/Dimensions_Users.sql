{{ config(materialized="view") }}

with
    base as (
        select
            u.user_id,
            u.display_name,
            u.reputation,
            u.location,
            u.country_guess,
            u.join_dt,
            u.last_access_dt
        from {{ ref("Silver_Layer_Users") }} u
        where u.user_id is not null
    ),
    ranked as (
        select
            b.*,
            row_number() over (
                partition by user_id
                order by last_access_dt desc nulls last, join_dt desc nulls last
            ) as rn
        from base b
    )
select
    user_id,
    display_name,
    reputation,
    location,
    country_guess,
    join_dt,
    last_access_dt,
    cast(null as string) as website_url_norm,
    date_diff(current_date(), join_dt, day) as tenure_days,
    (date_diff(current_date(), last_access_dt, day) <= 90) as is_active_90d
from ranked
where rn = 1
