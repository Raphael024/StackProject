{{ config(materialized='view') }}

WITH q AS (
  SELECT *
  FROM {{ source('so_raw','v_questions') }}
  WHERE question_id IS NOT NULL
)
SELECT
  
  (SELECT AS STRUCT q.*) AS raw_record,

  
  CAST(q.question_id AS INT64) AS question_id,

  
  NULLIF(TRIM(q.title), '') AS title,

 
  q.tags AS tags_raw,
  ARRAY(
    SELECT LOWER(TRIM(t))
    FROM UNNEST(SPLIT(COALESCE(q.tags, ''), '|')) AS t
    WHERE t IS NOT NULL AND t <> ''
  ) AS tags_array,

  
  COALESCE(q.question_url, CONCAT('https://stackoverflow.com/questions/', CAST(q.question_id AS STRING))) AS question_url,

  
  SAFE_CAST(q.creation_date       AS TIMESTAMP) AS creation_ts,
  DATE(q.creation_date)                          AS creation_dt,
  SAFE_CAST(q.last_activity_date  AS TIMESTAMP) AS last_activity_ts,
  DATE(q.last_activity_date)                     AS last_activity_dt,
  SAFE_CAST(q.last_edit_date      AS TIMESTAMP) AS last_edit_ts,
  DATE(q.last_edit_date)                         AS last_edit_dt,

  
  SAFE_CAST(q.accepted_answer_id AS INT64) AS accepted_answer_id,
  COALESCE(
    CAST(q.is_answered AS BOOL),
    q.accepted_answer_id IS NOT NULL OR SAFE_CAST(q.answer_count AS INT64) > 0
  ) AS is_answered,
  SAFE_CAST(q.answer_count    AS INT64) AS answer_count,
  SAFE_CAST(q.comment_count   AS INT64) AS comment_count,
  SAFE_CAST(q.favorite_count  AS INT64) AS favorite_count,
  SAFE_CAST(q.score           AS INT64) AS score,
  SAFE_CAST(q.view_count      AS INT64) AS view_count,


  CAST(q.owner_user_id AS INT64)         AS asker_user_id,
  NULLIF(TRIM(q.owner_display_name), '') AS asker_display_name
FROM q