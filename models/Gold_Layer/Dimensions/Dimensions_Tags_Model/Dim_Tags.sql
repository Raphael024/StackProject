-- models/Gold_Layer/Dim/dim_tags.sql
{{ config(materialized='table', cluster_by=['tag_id']) }}

WITH tags_from_questions AS (
  SELECT DISTINCT
    LOWER(TRIM(tag)) AS tag
  FROM {{ ref('stg_so_questions') }} q
  CROSS JOIN UNNEST(COALESCE(q.tags_array, ARRAY<STRING>[])) AS tag
  WHERE tag IS NOT NULL AND TRIM(tag) <> ''
),
tags_attrs AS (
  SELECT
    LOWER(TRIM(t.tag)) AS tag,
    SAFE_CAST(t.tag_count_raw AS INT64) AS tag_count_raw,
    t.excerpt_post_id,
    t.wiki_post_id
  FROM {{ ref('stg_so_tags') }} t
)

SELECT
  -- 👇 Explicit INT64 so it can never exceed 19 digits
  CAST(ABS(FARM_FINGERPRINT(tq.tag)) AS INT64) AS tag_id,
  tq.tag,
  ta.tag_count_raw,
  ta.excerpt_post_id,
  ta.wiki_post_id,
  REGEXP_CONTAINS(tq.tag, r'[^a-z0-9\-\+\#\.]') AS has_illegal_chars,
  IFNULL(ta.tag_count_raw, 0) = 0              AS is_zero_count
FROM tags_from_questions tq
LEFT JOIN tags_attrs ta USING (tag)
