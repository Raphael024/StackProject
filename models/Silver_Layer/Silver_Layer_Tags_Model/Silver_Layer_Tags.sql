{{ config(materialized="view") }}

WITH base AS (
  SELECT *
  FROM {{ source("DBT_RAW", "v_tags") }}
  WHERE tag IS NOT NULL
),

clean AS (
  SELECT
    (SELECT AS STRUCT b.*)                                 AS raw_record,
    LOWER(TRIM(b.tag))                                     AS tag,
    COALESCE(SAFE_CAST(b.tag_count      AS INT64), 0)      AS tag_count_raw,
    SAFE_CAST(b.excerpt_post_id AS INT64)                  AS excerpt_post_id,
    SAFE_CAST(b.wiki_post_id    AS INT64)                  AS wiki_post_id
  FROM base b
  WHERE TRIM(b.tag) IS NOT NULL AND TRIM(b.tag) <> ''
),

-- ensure one canonical row per tag
dedup AS (
  SELECT *
  FROM clean
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY tag
    ORDER BY tag_count_raw DESC, excerpt_post_id DESC, wiki_post_id DESC
  ) = 1
)

SELECT *
FROM dedup
