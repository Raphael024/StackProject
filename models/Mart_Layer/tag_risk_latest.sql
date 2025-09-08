{{ config(materialized='view') }}

WITH latest AS (
  SELECT MAX(creation_date) AS d
  FROM {{ ref('tag_health_daily') }}
),
agg AS (
  SELECT
    tag_id,
    ANY_VALUE(tag) AS tag,  -- tag text for display
    SUM(questions) AS questions,
    SUM(answers) AS answers,
    SAFE_DIVIDE(SUM(questions_with_any_answer), SUM(questions)) AS answer_rate,
    SAFE_DIVIDE(SUM(questions_with_accepted_answer), SUM(questions)) AS accepted_answer_rate,
    AVG(avg_days_to_first_answer) AS avg_days_to_first_answer
  FROM {{ ref('tag_health_daily') }}
  WHERE creation_date >= DATE_SUB((SELECT d FROM latest), INTERVAL 30 DAY)
  GROUP BY tag_id
)

SELECT
  tag_id,
  tag,
  questions,
  answers,
  answer_rate,
  accepted_answer_rate,
  avg_days_to_first_answer,
  questions * (1 - COALESCE(answer_rate, 0)) AS risk_score
FROM agg
WHERE questions >= 20  -- tune for your volume
ORDER BY risk_score DESC
