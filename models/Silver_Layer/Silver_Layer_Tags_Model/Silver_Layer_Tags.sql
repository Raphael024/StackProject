{{ config(materialized='view') }}

WITH t AS (
  SELECT *
  FROM {{ source('so_raw','v_tags') }}
  WHERE tag IS NOT NULL
)
SELECT

  (SELECT AS STRUCT t.*) AS raw_record,

  
  LOWER(TRIM(t.tag))          AS tag,
  SAFE_CAST(t.tag_count       AS INT64) AS tag_count_raw,
  SAFE_CAST(t.excerpt_post_id AS INT64) AS excerpt_post_id,
  SAFE_CAST(t.wiki_post_id    AS INT64) AS wiki_post_id
FROM t
