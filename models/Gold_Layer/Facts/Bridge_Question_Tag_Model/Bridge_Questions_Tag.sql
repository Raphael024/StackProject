
{{ config(materialized='view') }}

WITH pairs AS (
  SELECT DISTINCT
    q.question_id,
    LOWER(TRIM(tag)) AS tag
  FROM {{ ref('Silver_Layer_Questions') }} q
  CROSS JOIN UNNEST(COALESCE(q.tags_array, ARRAY<STRING>[])) AS tag
  WHERE tag != ''
)
SELECT
  p.question_id,
  d.tag_id
FROM pairs p
JOIN {{ ref('Dimensions_Tags') }} d USING (tag)
