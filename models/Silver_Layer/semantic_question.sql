{{ config(materialized='view') }}


WITH q AS (
  SELECT * FROM {{ ref('stg_so_questions') }}   -- note the double underscore
),
d AS (
  SELECT date_key, date FROM {{ ref('dim_date') }}
)

SELECT
  q.question_id,                 -- PK & FK → dim_questions
  q.asker_user_id,               -- FK → dim_users

  -- keep raw ts + derived dates
  q.creation_date                                  AS creation_ts,
  CAST(q.creation_date AS DATE)                    AS creation_date,
  DATE_TRUNC(CAST(q.creation_date AS DATE), MONTH) AS creation_month,
  q.last_activity_date                             AS last_activity_ts,
  CAST(q.last_activity_date AS DATE)               AS last_activity_date,

  -- date keys (align types to DATE)
  cd.date_key  AS creation_date_key,
  lad.date_key AS last_activity_date_key,

  -- measures
  q.answer_count,
  q.view_count,
  q.score,
  q.favorite_count,

  -- useful attrs
  q.accepted_answer_id,

  -- flags
  (q.answer_count > 0)               AS has_answers,
  (q.accepted_answer_id IS NOT NULL) AS has_accepted_answer

FROM q
LEFT JOIN d cd  ON cd.date  = CAST(q.creation_date AS DATE)
LEFT JOIN d lad ON lad.date = CAST(q.last_activity_date AS DATE);
