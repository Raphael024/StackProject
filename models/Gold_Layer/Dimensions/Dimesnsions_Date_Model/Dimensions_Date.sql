{{ config(materialized='table') }}

WITH dates AS (
  SELECT day AS date
  FROM UNNEST(GENERATE_DATE_ARRAY(DATE('2010-01-01'), CURRENT_DATE())) AS day
)
SELECT
  CAST(FORMAT_DATE('%Y%m%d', date) AS INT64) AS date_key,
  date,
  EXTRACT(YEAR      FROM date) AS year,
  EXTRACT(QUARTER   FROM date) AS quarter,
  EXTRACT(MONTH     FROM date) AS month,
  EXTRACT(DAY       FROM date) AS day,
  EXTRACT(ISOWEEK   FROM date) AS iso_week,
  EXTRACT(DAYOFWEEK FROM date) AS dow,
  (EXTRACT(DAYOFWEEK FROM date) IN (1,7))   AS is_weekend
FROM dates
