{{ config(materialized='view') }}

WITH a AS (
  SELECT * FROM {{ source('so_raw','v_answers') }}
  WHERE answer_id IS NOT NULL
)
SELECT
  (SELECT AS STRUCT a.*)                             AS raw_record,

  -- keys
  CAST(a.answer_id  AS INT64)                        AS answer_id,
  CAST(a.question_id AS INT64)                       AS question_id,

  -- dates
  SAFE_CAST(a.creation_date      AS TIMESTAMP)       AS creation_ts,
  DATE(a.creation_date)                              AS creation_date,
  SAFE_CAST(a.last_activity_date AS TIMESTAMP)       AS last_activity_ts,
  DATE(a.last_activity_date)                         AS last_activity_date,
  SAFE_CAST(a.last_edit_date     AS TIMESTAMP)       AS last_edit_ts,   -- remove if column doesn't exist

  -- measures
  SAFE_CAST(a.score         AS INT64)                AS score,
  SAFE_CAST(a.comment_count AS INT64)                AS comment_count,

  -- people
  CAST(a.owner_user_id AS INT64)                     AS answerer_user_id,
  NULLIF(TRIM(a.owner_display_name), '')             AS answerer_display_name,

  -- url (if present in your view; else build)
  COALESCE(a.answer_url,
           CONCAT('https://stackoverflow.com/a/', CAST(a.answer_id AS STRING))) AS answer_url,

  -- flag (if present in v_answers; harmlessly null if not)
  CAST(a.is_accepted AS BOOL)                        AS is_accepted
FROM a
