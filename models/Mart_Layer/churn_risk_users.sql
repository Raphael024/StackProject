{{ config(materialized='view') }}

WITH w AS (
  SELECT MAX(creation_date) AS d
  FROM {{ ref('facts_questions') }}
),
recent_q AS (
  SELECT
    asker_user_id AS user_id,
    COUNT(*) AS recent_questions,
    COUNTIF(has_answers) AS recent_questions_answered
  FROM {{ ref('facts_questions') }}
  WHERE creation_date >= DATE_SUB((SELECT d FROM w), INTERVAL 60 DAY)
  GROUP BY user_id
)

SELECT
  r.user_id,
  du.display_name,
  du.reputation,
  r.recent_questions,
  r.recent_questions_answered,
  SAFE_DIVIDE(r.recent_questions_answered, NULLIF(r.recent_questions, 0)) AS recent_answer_rate
FROM recent_q r
LEFT JOIN {{ ref('dim_users') }} du USING (user_id)
WHERE r.recent_questions >= 3
  AND COALESCE(
        SAFE_DIVIDE(r.recent_questions_answered, NULLIF(r.recent_questions, 0)),
        0
      ) < 0.34
ORDER BY recent_answer_rate ASC, r.recent_questions DESC
