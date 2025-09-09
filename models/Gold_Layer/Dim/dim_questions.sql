{{ config(materialized='table') }}

SELECT
  CAST(q.question_id AS INT64)                                AS question_id,   -- PK
  NULLIF(TRIM(q.title), '')                                   AS title,
  COALESCE(q.question_url,
           CONCAT('https://stackoverflow.com/questions/', CAST(q.question_id AS STRING)))
                                                              AS question_url
FROM {{ source('so_raw','v_questions') }} q
WHERE q.question_id IS NOT NULL
