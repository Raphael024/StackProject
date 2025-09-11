{{ config(materialized="view") }}

with
    base as (
        select
            q.question_id,
            q.title,
            coalesce(
                q.question_url,
                concat(
                    'https://stackoverflow.com/questions/',
                    cast(q.question_id as string)
                )
            ) as question_url,
            q.creation_ts,
            q.last_activity_ts
        from {{ ref("Silver_Layer_Questions") }} q
        where q.question_id is not null
    )
select question_id, title, question_url
from base
qualify
    row_number() over (
        partition by question_id
        order by last_activity_ts desc nulls last, creation_ts desc nulls last
    )
    = 1
