{{ config(materialized='table') }}

WITH base AS (
  SELECT
    q.question_id,
    q.title,
    COALESCE(q.question_url,
             CONCAT('https://stackoverflow.com/questions/', CAST(q.question_id AS STRING))) AS question_url,
    q.creation_ts,
    q.last_activity_ts
  FROM {{ ref('stg_so_questions') }} q
  WHERE q.question_id IS NOT NULL
),
ranked AS (
  SELECT
    b.*,
    ROW_NUMBER() OVER (
      PARTITION BY question_id
      ORDER BY last_activity_ts DESC NULLS LAST, creation_ts DESC NULLS LAST
    ) AS rn
  FROM base b
)
SELECT
  question_id,
  title,
  question_url
FROM ranked
WHERE rn = 1
