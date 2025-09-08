{{ config(materialized='view') }}

WITH monthly AS (
  SELECT
    tag_id,
    tag,
    DATE_TRUNC(creation_date, MONTH) AS month_start,
    SUM(questions) AS questions
  FROM {{ ref('tag_health_daily') }}
  GROUP BY 1,2,3
),
with_prev AS (
  SELECT
    tag_id,
    tag,
    month_start,
    questions AS questions_curr,
    LAG(questions) OVER (PARTITION BY tag_id ORDER BY month_start) AS questions_prev
  FROM monthly
)
SELECT
  tag_id,
  tag,
  month_start,
  questions_curr,
  questions_prev,
  SAFE_DIVIDE(questions_curr - questions_prev, NULLIF(questions_prev, 0)) AS mom_growth
FROM with_prev
