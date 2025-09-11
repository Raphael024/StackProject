{{ config(materialized="view") }}

with
    u as (select * from {{ source("DBT_RAW", "v_users") }} where user_id is not null),
    split_loc as (select u.*, split(u.location, ',') as loc_parts from u)
select
    (select as struct s.* except (loc_parts)) as raw_record,

    cast(s.user_id as int64) as user_id,

    nullif(trim(s.display_name), '') as display_name,
    safe_cast(s.reputation as int64) as reputation,
    nullif(trim(s.location), '') as location,
    case
        when array_length(loc_parts) > 0
        then nullif(trim(loc_parts[offset(array_length(loc_parts) - 1)]), '')
        else null
    end as country_guess,

    safe_cast(s.creation_date as timestamp) as join_ts,
    date(s.creation_date) as join_dt,
    safe_cast(s.last_access_date as timestamp) as last_access_ts,
    date(s.last_access_date) as last_access_dt
from split_loc as s
