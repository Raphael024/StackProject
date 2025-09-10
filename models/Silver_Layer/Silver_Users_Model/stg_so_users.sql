{{ config(materialized='view') }}

WITH u AS (
  SELECT *
  FROM {{ source('so_raw','v_users') }}
  WHERE user_id IS NOT NULL
),
split_loc AS (
  SELECT
    u.*,
    SPLIT(u.location, ',') AS loc_parts
  FROM u
)
SELECT
  -- raw payload
  (SELECT AS STRUCT s.* EXCEPT(loc_parts)) AS raw_record,

  -- keys
  CAST(s.user_id AS INT64) AS user_id,

  -- attributes
  NULLIF(TRIM(s.display_name), '') AS display_name,
  SAFE_CAST(s.reputation AS INT64) AS reputation,
  NULLIF(TRIM(s.location), '')     AS location,
  CASE
    WHEN ARRAY_LENGTH(loc_parts) > 0
      THEN NULLIF(TRIM(loc_parts[OFFSET(ARRAY_LENGTH(loc_parts) - 1)]), '')
    ELSE NULL
  END AS country_guess,

  -- dates
  SAFE_CAST(s.creation_date    AS TIMESTAMP) AS join_ts,
  DATE(s.creation_date)                      AS join_dt,
  SAFE_CAST(s.last_access_date AS TIMESTAMP) AS last_access_ts,
  DATE(s.last_access_date)                   AS last_access_dt
FROM split_loc AS s
