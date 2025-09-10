{{ config(materialized='view') }}

WITH q AS (
  SELECT
    question_id,
    asker_user_id,
    accepted_answer_id,
    -- Canonical DATE fields from the actual Silver names
    creation_date      AS creation_dt,
    last_activity_date AS last_activity_dt,
    view_count,
    score,
    favorite_count
  FROM {{ ref('stg_so_questions') }}
  WHERE question_id IS NOT NULL
),

answers_per_q AS (
  -- Pre-aggregate to question grain (this is your COUNT DISTINCT IDs)
  SELECT
    question_id,
    COUNT(DISTINCT answer_id) AS answer_count_calc
  FROM {{ ref('stg_so_answers') }}
  GROUP BY question_id
),

d AS (
  SELECT date_key, date
  FROM {{ ref('dim_date') }}
)

SELECT
  q.question_id,
  q.asker_user_id,
  q.accepted_answer_id,

  -- Dates (DATE)
  q.creation_dt,
  q.last_activity_dt,
  DATE_TRUNC(q.creation_dt, MONTH) AS creation_month,

  -- Date keys
  cd.date_key  AS creation_date_key,
  lad.date_key AS last_activity_date_key,

  -- Measures (use pre-agg count to avoid fan-out)
  COALESCE(apq.answer_count_calc, 0) AS answer_count,
  q.view_count,
  q.score,
  q.favorite_count,

  -- Flags
  (COALESCE(apq.answer_count_calc, 0) > 0) AS has_answers,
  (q.accepted_answer_id IS NOT NULL)       AS has_accepted_answer

FROM q
LEFT JOIN answers_per_q apq USING (question_id)
LEFT JOIN d cd  ON cd.date  = q.creation_dt
LEFT JOIN d lad ON lad.date = q.last_activity_dt
