{{ config(
  materialized='table',
  partition_by={'field':'creation_date','data_type':'date'},
  cluster_by=['question_id','answerer_user_id']
) }}

WITH a AS (SELECT * FROM {{ ref('stg_so_answers') }}),
q AS (SELECT question_id, accepted_answer_id FROM {{ ref('stg_so_questions') }}),
d AS (SELECT date_key, date FROM {{ ref('dim_date') }})

SELECT
  a.answer_id,                               -- PK
  a.question_id,                             -- FK → dim_questions / fact_questions
  a.answerer_user_id,                        -- FK → dim_users
  cd.date_key  AS creation_date_key,         -- FK → dim_date
  lad.date_key AS last_activity_date_key,    -- FK → dim_date

  -- measures
  a.score,
  a.comment_count,

  -- spec flag (robust even if source lacks is_accepted)
  (a.answer_id = q.accepted_answer_id) AS is_accepted

FROM a
LEFT JOIN q  USING (question_id)
LEFT JOIN d cd  ON cd.date  = a.creation_date
LEFT JOIN d lad ON lad.date = a.last_activity_date
