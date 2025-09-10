{{ config(materialized='table') }}

SELECT
  u.user_id,                 
  u.display_name,
  u.reputation,
  u.location,
  u.country_guess,
  u.join_date,
  u.last_access_date,

  
  CAST(NULL AS STRING) AS website_url_norm,

  DATE_DIFF(CURRENT_DATE(), u.join_date, DAY)                 AS tenure_days,
  (DATE_DIFF(CURRENT_DATE(), u.last_access_date, DAY) <= 90)  AS is_active_90d
FROM {{ ref('Silver_Layer_Users') }} u