{{ config(materialized="view", alias="major_report") }}

with
    q as (
        select
            question_id,
            asker_user_id,
            accepted_answer_id,
            creation_date_key,
            last_activity_date_key,
            answer_count,
            view_count,
            score,
            favorite_count,
            has_answers,
            has_accepted_answer
        from {{ ref("Facts_Questions") }}
    ),

    dq as (
        select question_id, title, question_url from {{ ref("Dimensions_Questions") }}
    ),

    asker as (
        select
            user_id as asker_user_id,
            display_name as asker_name,
            reputation as asker_reputation,
            country_guess as asker_country
        from {{ ref("Dimensions_Users") }}
    ),

    accepted_ans as (
        select question_id, answer_id, answerer_user_id
        from
            (
                select
                    a.*,
                    row_number() over (
                        partition by question_id order by answer_id
                    ) as rn
                from {{ ref("Facts_Answers") }} a
                where a.is_accepted
            )
        where rn = 1
    ),

    accepted_user as (
        select
            user_id as accepted_user_id,
            display_name as accepted_user_name,
            reputation as accepted_user_reputation
        from {{ ref("Dimensions_Users") }}
    ),

    
    tags as (
        select
            b.question_id,
            array_agg(distinct t.tag order by t.tag) as tag_ids,  -- using tag names as IDs
            array_agg(distinct t.tag order by t.tag) as tags_array,
            string_agg(distinct t.tag, '|' order by t.tag) as tags_csv,
            count(distinct t.tag) as tag_count
        from {{ ref("Bridge_Questions_Tag") }} b
        join {{ ref("Dimensions_Tags") }} t on t.tag = b.tag
        group by b.question_id
    ),

    cd as (
        select
            date_key,
            date as creation_dt,
            extract(year from date) as creation_year,
            extract(month from date) as creation_month_num,
            extract(quarter from date) as creation_quarter
        from {{ ref("Dimensions_Date") }}
    ),
    lad as (select date_key, date as last_activity_dt from {{ ref("Dimensions_Date") }})

select
    count(distinct q.question_id) as distinct_count_of_question_id,
    dq.title,
    dq.question_url,
    count(distinct q.asker_user_id) as distinct_count_of_asker_user_id,
    ak.asker_name,
    ak.asker_reputation,
    ak.asker_country,
    cd.creation_dt,
    lad.last_activity_dt,
    date_trunc(cd.creation_dt, month) as creation_month,
    q.creation_date_key,
    q.last_activity_date_key,
    cd.creation_year,
    cd.creation_month_num,
    cd.creation_quarter,
    sum(q.answer_count) as sum_of_answer_count,
    sum(q.view_count) as sum_of_view_count,
    q.score,
    sum(q.favorite_count) as sum_of_favorite_count,
    q.has_answers,
    q.has_accepted_answer,
    count(distinct q.accepted_answer_id) as distinct_count_of_accepted_answer_id,
    count(distinct aa.answer_id) as distinct_count_of_answerer_id,
    count(distinct aa.answerer_user_id) as distinct_count_of_answerer_user_id,
    au.accepted_user_name,
    au.accepted_user_reputation,
    t.tags_array,
    t.tags_csv
from q
left join dq using (question_id)
left join asker ak using (asker_user_id)
left join accepted_ans aa using (question_id)
left join accepted_user au on au.accepted_user_id = aa.answerer_user_id
left join tags t on t.question_id = q.question_id
left join cd on cd.date_key = q.creation_date_key
left join lad on lad.date_key = q.last_activity_date_key
group by 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 18, 20, 21, 25, 26, 27, 28
