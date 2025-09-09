{{ config(materialized='view') }}

WITH u AS (
  SELECT * FROM {{ source('so_raw','v_users') }}
  WHERE user_id IS NOT NULL
)
SELECT
  (SELECT AS STRUCT u.*)                             AS raw_record,

  -- keys
  CAST(u.user_id AS INT64)                           AS user_id,

  -- attributes
  NULLIF(TRIM(u.display_name), '')                   AS display_name,
  SAFE_CAST(u.reputation AS INT64)                   AS reputation,
  NULLIF(TRIM(u.location), '')                       AS location,
  NULLIF(TRIM(REGEXP_EXTRACT(u.location, '([^,]+)$')), '') AS country_guess,

  -- dates
  SAFE_CAST(u.creation_date    AS TIMESTAMP)         AS join_ts,
  DATE(u.creation_date)                              AS join_date,
  SAFE_CAST(u.last_access_date AS TIMESTAMP)         AS last_access_ts,
  DATE(u.last_access_date)                           AS last_access_date

  -- If your v_users DOES have these, you can uncomment:
  -- , SAFE_CAST(u.up_votes  AS INT64)                 AS up_votes
  -- , SAFE_CAST(u.down_votes AS INT64)                AS down_votes
  -- , SAFE_CAST(u.views     AS INT64)                 AS profile_views
  -- , CASE
  --     WHEN u.website_url IS NULL OR TRIM(u.website_url) = '' THEN NULL
  --     WHEN REGEXP_CONTAINS(LOWER(u.website_url), r'^(https?|ftp)://') THEN u.website_url
  --     ELSE CONCAT('http://', u.website_url)
  --   END                                            AS website_url_norm
FROM u
