{{ config(materialized='view', alias='major_report') }}


WITH q AS (
  SELECT
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
  FROM {{ ref('Facts_Questions') }}
),


dq AS (
  SELECT
    question_id,
    title,
    question_url
  FROM {{ ref('Dimensions_Questions') }}
),


asker AS (
  SELECT
    user_id        AS asker_user_id,
    display_name   AS asker_name,
    reputation     AS asker_reputation,
    country_guess  AS asker_country
  FROM {{ ref('Dimensions_Users') }}
),

accepted_ans AS (
  SELECT
    question_id,
    answer_id,
    answerer_user_id
  FROM (
    SELECT
      a.*,
      ROW_NUMBER() OVER (
        PARTITION BY question_id
        ORDER BY answer_id   -- deterministic; avoids referencing date columns
      ) AS rn
    FROM {{ ref('Answers') }} a
    WHERE a.is_accepted
  )
  WHERE rn = 1
),

accepted_user AS (
  SELECT
    user_id       AS accepted_user_id,
    display_name  AS accepted_user_name,
    reputation    AS accepted_user_reputation
  FROM {{ ref('Dimensions_users') }}
),


tags AS (
  SELECT
    b.question_id,
    ARRAY_AGG(DISTINCT t.tag_id ORDER BY t.tag_id) AS tag_ids,
    ARRAY_AGG(DISTINCT t.tag    ORDER BY t.tag)    AS tags_array,
    STRING_AGG(DISTINCT t.tag, '|' ORDER BY t.tag) AS tags_csv,
    COUNT(DISTINCT t.tag_id)                       AS tag_count
  FROM {{ ref('Bridge_Questions_Tag') }} b
  JOIN {{ ref('Dimensions_Tags') }} t USING (tag_id)
  GROUP BY b.question_id
),


cd AS (
  SELECT
    date_key,
    date AS creation_dt,
    EXTRACT(YEAR    FROM date) AS creation_year,
    EXTRACT(MONTH   FROM date) AS creation_month_num,
    EXTRACT(QUARTER FROM date) AS creation_quarter
  FROM {{ ref('Dimensions_Date') }}
),
lad AS (
  SELECT
    date_key,
    date AS last_activity_dt
  FROM {{ ref('Dimensions_Date') }}
)


SELECT
  q.question_id,


  dq.title,
  dq.question_url,


  q.asker_user_id,
  ak.asker_name,
  ak.asker_reputation,
  ak.asker_country,

 
  cd.creation_dt,
  lad.last_activity_dt,
  DATE_TRUNC(cd.creation_dt, MONTH) AS creation_month,
  q.creation_date_key,
  q.last_activity_date_key,
  cd.creation_year,
  cd.creation_month_num,
  cd.creation_quarter,

  
  q.answer_count,   
  q.view_count,
  q.score,
  q.favorite_count,
  q.has_answers,
  q.has_accepted_answer,

  
  q.accepted_answer_id                             AS accepted_answer_id_from_q,
  aa.answer_id                                     AS accepted_answer_id_resolved,
  aa.answerer_user_id                              AS accepted_answerer_user_id,
  au.accepted_user_name,
  au.accepted_user_reputation,

  
  t.tag_count,
  t.tags_array,
  t.tags_csv,
  t.tag_ids

FROM q
LEFT JOIN dq              USING (question_id)
LEFT JOIN asker      ak   USING (asker_user_id)
LEFT JOIN accepted_ans aa USING (question_id)
LEFT JOIN accepted_user au ON au.accepted_user_id = aa.answerer_user_id
LEFT JOIN tags        t   USING (question_id)
LEFT JOIN cd               ON cd.date_key  = q.creation_date_key
LEFT JOIN lad              ON lad.date_key = q.last_activity_date_key