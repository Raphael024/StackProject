{{ config(materialized='table') }}

SELECT
  ABS(FARM_FINGERPRINT(t.tag)) AS tag_id,  -- surrogate PK
  t.tag,
  t.tag_count_raw,                         -- from Silver; if your Silver uses tag_count, rename there
  -- validation bits as attributes
  REGEXP_CONTAINS(t.tag, r'[^a-z0-9\-\+\#\.]') AS has_illegal_chars,
  (t.tag_count_raw = 0)                         AS is_zero_count,
  t.excerpt_post_id,
  t.wiki_post_id
FROM {{ ref('stg_so_tags') }} t
GROUP BY tag_id, t.tag, t.tag_count_raw, has_illegal_chars, is_zero_count, t.excerpt_post_id, t.wiki_post_id
