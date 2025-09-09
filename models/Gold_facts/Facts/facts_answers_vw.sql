{{ config(materialized='view') }}

WITH a AS ( SELECT * FROM {{ ref('stg_so_answers') }} ),
q AS ( SELECT question_id, accepted_answer_id FROM {{ ref('stg_so_questions') }} ),
d AS ( SELECT date_key, date FROM {{ ref('dim_date') }} )

SELECT
  a.answer_id,
  a.question_id,
  a.answerer_user_id,
  a.creation_date,
  DATE_TRUNC(a.creation_date, MONTH) AS creation_month,
  a.last_activity_date,
  cd.date_key  AS creation_date_key,
  lad.date_key AS last_activity_date_key,
  a.score,
  a.comment_count,
  (a.answer_id = q.accepted_answer_id) AS is_accepted
FROM a
LEFT JOIN q  USING (question_id)
LEFT JOIN d cd  ON cd.date  = a.creation_date
LEFT JOIN d lad ON lad.date = a.last_activity_date
