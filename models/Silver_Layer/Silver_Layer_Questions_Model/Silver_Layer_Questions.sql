{{ config(materialized="view") }}

with
    q as (
        select *
        from {{ source("DBT_RAW", "v_questions") }}
        where question_id is not null
    )
select
    (select as struct q.*) as raw_record,
    cast(q.question_id as int64) as question_id,
    nullif(trim(q.title), '') as title,
    q.tags as tags_raw,
    array(
        select lower(trim(t))
        from unnest(split(coalesce(q.tags, ''), '|')) as t
        where t is not null and t <> ''
    ) as tags_array,

    coalesce(
        q.question_url,
        concat('https://stackoverflow.com/questions/', cast(q.question_id as string))
    ) as question_url,
    safe_cast(q.creation_date as timestamp) as creation_ts,
    date(q.creation_date) as creation_dt,
    safe_cast(q.last_activity_date as timestamp) as last_activity_ts,
    date(q.last_activity_date) as last_activity_dt,
    safe_cast(q.last_edit_date as timestamp) as last_edit_ts,
    date(q.last_edit_date) as last_edit_dt,

    safe_cast(q.accepted_answer_id as int64) as accepted_answer_id,
    coalesce(
        cast(q.is_answered as bool),
        q.accepted_answer_id is not null or safe_cast(q.answer_count as int64) > 0
    ) as is_answered,
    safe_cast(q.answer_count as int64) as answer_count,
    safe_cast(q.comment_count as int64) as comment_count,
    safe_cast(q.favorite_count as int64) as favorite_count,
    safe_cast(q.score as int64) as score,
    safe_cast(q.view_count as int64) as view_count,

    cast(q.owner_user_id as int64) as asker_user_id,
    nullif(trim(q.owner_display_name), '') as asker_display_name
from q
