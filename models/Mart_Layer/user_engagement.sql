{{ config(materialized='view') }}

WITH q AS (
  SELECT
    asker_user_id AS user_id,
    COUNT(*) AS questions_asked
  FROM {{ ref('facts_questions') }}
  GROUP BY 1
),
a AS (
  SELECT
    answerer_user_id AS user_id,
    COUNT(*) AS answers_given,
    COUNTIF(is_accepted) AS accepted_answers
  FROM {{ ref('facts_answers') }}
  GROUP BY 1
),
u AS (
  -- Use the union of all participants so we don't miss users not present in dim_users
  SELECT DISTINCT user_id FROM (
    SELECT asker_user_id AS user_id FROM {{ ref('facts_questions') }}
    UNION ALL
    SELECT answerer_user_id AS user_id FROM {{ ref('facts_answers') }}
    UNION ALL
    SELECT user_id FROM {{ ref('dim_users') }}
  )
)

SELECT
  u.user_id,
  du.display_name,
  du.reputation,
  COALESCE(q.questions_asked, 0)  AS questions_asked,
  COALESCE(a.answers_given, 0)    AS answers_given,
  COALESCE(a.accepted_answers, 0) AS accepted_answers,
  SAFE_DIVIDE(COALESCE(a.accepted_answers, 0), NULLIF(COALESCE(a.answers_given, 0), 0)) AS answer_accept_rate
FROM u
LEFT JOIN q  USING (user_id)
LEFT JOIN a  USING (user_id)
LEFT JOIN {{ ref('dim_users') }} du USING (user_id)
