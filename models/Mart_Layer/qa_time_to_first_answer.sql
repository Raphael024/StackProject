{{ config(materialized='view') }}

WITH first_answer AS (
  SELECT
    question_id,
    MIN(creation_date) AS first_answer_date
  FROM {{ ref('facts_answers') }}
  GROUP BY question_id
)

SELECT
  q.question_id,
  q.creation_date AS question_date,
  fa.first_answer_date,
  IF(
    fa.first_answer_date IS NULL,
    NULL,
    DATE_DIFF(fa.first_answer_date, q.creation_date, DAY)
  ) AS days_to_first_answer,
  q.has_answers AS has_any_answer
FROM {{ ref('facts_questions') }} AS q
LEFT JOIN first_answer AS fa USING (question_id)
