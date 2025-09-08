{{ config(materialized='view') }}

-- Tag popularity (question volume) vs answers/answer rate
WITH agg AS (
  SELECT
    tag_id,
    ANY_VALUE(tag) AS tag,
    SUM(questions) AS questions,
    SUM(questions_with_any_answer) AS questions_answered,
    SUM(answers) AS answers
  FROM {{ ref('tag_health_daily') }}
  GROUP BY tag_id
)

SELECT
  tag_id,
  tag,
  questions,                 -- total questions (popularity)
  questions_answered,        -- questions that received ≥1 answer
  answers,                   -- total number of answers posted
  SAFE_DIVIDE(questions_answered, NULLIF(questions, 0)) AS answer_rate
FROM agg
