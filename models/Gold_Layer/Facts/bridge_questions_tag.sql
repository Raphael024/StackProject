{{ config(materialized='table', cluster_by=['tag_id']) }}

WITH pairs AS (
  SELECT
    q.question_id,
    LOWER(TRIM(t)) AS tag
  FROM {{ ref('stg_so_questions') }} q,
  UNNEST(IFNULL(q.tags_array, ARRAY<STRING>[])) AS t
  WHERE t IS NOT NULL AND t != ''
),
dt AS (
  SELECT
    tag_id,
    LOWER(TRIM(tag)) AS tag
  FROM {{ ref('dim_tags') }}
)

SELECT DISTINCT
  p.question_id,
  d.tag_id
FROM pairs p
JOIN dt d USING (tag)
