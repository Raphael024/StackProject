{{ config(materialized='table', cluster_by=['tag_id']) }}

WITH pairs AS (
  SELECT
    q.question_id,
    t AS tag
  FROM {{ ref('stg_so_questions') }} q,
  UNNEST(q.tags_array) t
),
dt AS (SELECT tag_id, tag FROM {{ ref('dim_tags') }})

SELECT
  p.question_id,
  d.tag_id
FROM pairs p
JOIN dt d USING (tag)
