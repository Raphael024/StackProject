{{ config(materialized="view") }}

WITH pairs AS (
  SELECT DISTINCT
    q.question_id,
    LOWER(TRIM(tag)) AS tag
  FROM {{ ref("Silver_Layer_Questions") }} q
  CROSS JOIN UNNEST(COALESCE(q.tags_array, ARRAY<STRING>[])) AS tag
  WHERE tag IS NOT NULL AND tag <> ''
)

SELECT
  p.question_id,
  dt.tag_id
FROM pairs p
JOIN {{ ref("Dimensions_Tags") }} dt
  USING (tag)
