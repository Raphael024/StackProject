{{ config(materialized='table') }}

SELECT
  u.user_id,                 -- PK
  u.display_name,
  u.reputation,
  u.location,
  u.country_guess,
  u.join_date,
  u.last_access_date,

  -- Spec fields:
  -- website_url_norm may not exist in Silver; keep the column and populate when available
  CAST(NULL AS STRING) AS website_url_norm,

  DATE_DIFF(CURRENT_DATE(), u.join_date, DAY)                 AS tenure_days,
  (DATE_DIFF(CURRENT_DATE(), u.last_access_date, DAY) <= 90)  AS is_active_90d
FROM {{ ref('stg_so_users') }} u
