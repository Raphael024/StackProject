{{ config(schema="Gold_layer", materialized="view") }}

with
    a as (
        select
            a.answer_id,
            a.question_id,
            a.answerer_user_id,
            a.creation_dt,
            a.last_activity_dt,
            a.score,
            a.comment_count
        from {{ ref("Silver_Layer_Answers") }} a
        where a.answer_id is not null
    ),
    q as (
        select question_id, accepted_answer_id from {{ ref("Silver_Layer_Questions") }}
    ),
    d as (select date_key, date from {{ ref("Dimensions_Date") }})
select
    a.answer_id,
    a.question_id,
    a.answerer_user_id,
    a.creation_dt,
    a.last_activity_dt,
    date_trunc(a.creation_dt, month) as creation_month,
    cd.date_key as creation_date_key,
    lad.date_key as last_activity_date_key,
    a.score,
    a.comment_count,
    coalesce(a.answer_id = q.accepted_answer_id, false) as is_accepted
from a
left join q using (question_id)
left join d cd on cd.date = a.creation_dt
left join d lad on lad.date = a.last_activity_dt
