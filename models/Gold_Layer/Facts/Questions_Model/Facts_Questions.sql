{{ config(schema="Gold_layer", materialized="view") }}
with
    q as (
        select
            question_id,
            asker_user_id,
            accepted_answer_id,
            creation_dt,
            last_activity_dt,
            view_count,
            score,
            comment_count,
            favorite_count
        from {{ ref("Silver_Layer_Questions") }}
        where question_id is not null
    ),
    answers_per_q as (
        select question_id, count(distinct answer_id) as answer_count_calc
        from {{ ref("Silver_Layer_Answers") }}
        group by question_id
    ),
    d as (select date_key, date from {{ ref("Dimensions_Date") }})
select
    q.question_id,
    q.asker_user_id,
    q.accepted_answer_id,
    q.creation_dt,
    q.last_activity_dt,
    q.comment_count,
    date_trunc(q.creation_dt, month) as creation_month,
    cd.date_key as creation_date_key,
    lad.date_key as last_activity_date_key,
    coalesce(apq.answer_count_calc, 0) as answer_count,
    q.view_count,
    q.score,
    q.favorite_count,
    (coalesce(apq.answer_count_calc, 0) > 0) as has_answers,
    (q.accepted_answer_id is not null) as has_accepted_answer
from q
left join answers_per_q apq using (question_id)
left join d cd on cd.date = q.creation_dt
left join d lad on lad.date = q.last_activity_dt
