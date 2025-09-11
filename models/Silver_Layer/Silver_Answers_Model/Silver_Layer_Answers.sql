{{ config(materialized="view") }}

with
    a as (
        select * from {{ source("DBT_RAW", "v_answers") }} where answer_id is not null
    )
select
    (select as struct a.*) as raw_record,
    cast(a.answer_id as int64) as answer_id,
    cast(a.question_id as int64) as question_id,
    safe_cast(a.creation_date as timestamp) as creation_ts,
    date(a.creation_date) as creation_dt,
    safe_cast(a.last_activity_date as timestamp) as last_activity_ts,
    date(a.last_activity_date) as last_activity_dt,
    safe_cast(a.last_edit_date as timestamp) as last_edit_ts,
    date(a.last_edit_date) as last_edit_dt,
    safe_cast(a.score as int64) as score,
    safe_cast(a.comment_count as int64) as comment_count,
    cast(a.owner_user_id as int64) as answerer_user_id,
    nullif(trim(a.owner_display_name), '') as answerer_display_name,
    coalesce(
        a.answer_url,
        concat('https://stackoverflow.com/a/', cast(a.answer_id as string))
    ) as answer_url,
    cast(a.is_accepted as bool) as is_accepted
from a
