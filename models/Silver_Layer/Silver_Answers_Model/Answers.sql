{{ config(materialized='view') }}

WITH a AS (
  SELECT *
  FROM {{ source('so_raw','v_answers') }}
  WHERE answer_id IS NOT NULL
)
SELECT
  -- raw payload
  (SELECT AS STRUCT a.*) AS raw_record,

  -- keys
  CAST(a.answer_id   AS INT64) AS answer_id,
  CAST(a.question_id AS INT64) AS question_id,

  -- dates
  SAFE_CAST(a.creation_date       AS TIMESTAMP) AS creation_ts,
  DATE(a.creation_date)                          AS creation_dt,
  SAFE_CAST(a.last_activity_date  AS TIMESTAMP) AS last_activity_ts,
  DATE(a.last_activity_date)                     AS last_activity_dt,
  SAFE_CAST(a.last_edit_date      AS TIMESTAMP) AS last_edit_ts,
  DATE(a.last_edit_date)                         AS last_edit_dt,

  -- measures
  SAFE_CAST(a.score         AS INT64) AS score,
  SAFE_CAST(a.comment_count AS INT64) AS comment_count,

  -- people
  CAST(a.owner_user_id AS INT64)        AS answerer_user_id,
  NULLIF(TRIM(a.owner_display_name), '') AS answerer_display_name,

  -- url (fallback if not present in raw)
  COALESCE(a.answer_url, CONCAT('https://stackoverflow.com/a/', CAST(a.answer_id AS STRING))) AS answer_url,

  -- flags
  CAST(a.is_accepted AS BOOL) AS is_accepted
FROM a
