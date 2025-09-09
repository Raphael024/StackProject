{{ config(materialized='view') }}

WITH q AS ( SELECT * FROM {{ ref('stg_so_questions') }} ),
d AS ( SELECT date_key, date FROM {{ ref('dim_date') }} )

SELECT
  q.question_id,
  q.asker_user_id,
  q.accepted_answer_id,
  q.creation_date,
  q.last_activity_date,
  DATE_TRUNC(q.creation_date, MONTH) AS creation_month,
  cd.date_key  AS creation_date_key,
  lad.date_key AS last_activity_date_key,
  q.answer_count,
  q.view_count,
  q.score,
  q.favorite_count,
  (q.answer_count > 0)               AS has_answers,
  (q.accepted_answer_id IS NOT NULL) AS has_accepted_answer
FROM q
LEFT JOIN d cd  ON cd.date  = q.creation_date
LEFT JOIN d lad ON lad.date = q.last_activity_date
