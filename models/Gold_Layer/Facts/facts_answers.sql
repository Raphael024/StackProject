-- models/Gold_Layer/Facts/facts_answers.sql
{{ config(
  materialized='table',
  partition_by={'field':'creation_month','data_type':'date'},
  cluster_by=['question_id','answerer_user_id']
) }}

WITH a AS (SELECT * FROM {{ ref('stg_so_answers') }}),
q AS (SELECT question_id, accepted_answer_id FROM {{ ref('stg_so_questions') }}),
d AS (SELECT date_key, date FROM {{ ref('dim_date') }})

SELECT
  a.answer_id,                                -- PK
  a.question_id,                              -- FK
  a.answerer_user_id,                         -- FK

  -- keep raw dates + a monthly partitioning key
  a.creation_date,
  DATE_TRUNC(a.creation_date, MONTH) AS creation_month,   -- <-- partition column
  a.last_activity_date,

  -- date keys
  cd.date_key  AS creation_date_key,
  lad.date_key AS last_activity_date_key,

  -- measures
  a.score,
  a.comment_count,

  -- acceptance flag (robust even if source lacks it)
  (a.answer_id = q.accepted_answer_id) AS is_accepted
FROM a
LEFT JOIN q  USING (question_id)
LEFT JOIN d cd  ON cd.date  = a.creation_date
LEFT JOIN d lad ON lad.date = a.last_activity_date
