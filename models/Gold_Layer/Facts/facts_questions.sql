{{ config(
  materialized='table',
  partition_by={'field':'creation_date','data_type':'date'},
  cluster_by=['asker_user_id','accepted_answer_id']
) }}

WITH q AS (SELECT * FROM {{ ref('stg_so_questions') }}),
d AS (SELECT date_key, date FROM {{ ref('dim_date') }})

SELECT
  q.question_id,                              -- PK & FK → dim_questions
  q.asker_user_id,                            -- FK → dim_users
  cd.date_key  AS creation_date_key,          -- FK → dim_date
  lad.date_key AS last_activity_date_key,     -- FK → dim_date

  -- measures
  q.answer_count,
  q.view_count,
  q.score,
  q.favorite_count,

  -- convenience flags (per spec)
  (q.answer_count > 0)                 AS has_answers,
  (q.accepted_answer_id IS NOT NULL)   AS has_accepted_answer

FROM q
LEFT JOIN d cd  ON cd.date  = q.creation_date
LEFT JOIN d lad ON lad.date = q.last_activity_date
