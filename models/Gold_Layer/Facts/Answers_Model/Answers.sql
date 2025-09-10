-- models/Gold_facts/Facts/facts_answers_vw.sql
{{ config(materialized='view') }}

WITH a AS (
  SELECT
    a.answer_id,
    a.question_id,
    a.answerer_user_id,
    -- Use the actual Silver columns and alias them to canonical names
    a.creation_date      AS creation_dt,        -- DATE
    a.last_activity_date AS last_activity_dt,   -- DATE
    a.score,
    a.comment_count
  FROM {{ ref('stg_so_answers') }} a
  WHERE a.answer_id IS NOT NULL
),
q AS (
  SELECT
    question_id,
    accepted_answer_id
  FROM {{ ref('stg_so_questions') }}
),
d AS (
  SELECT date_key, date
  FROM {{ ref('dim_date') }}
)

SELECT
  a.answer_id,
  a.question_id,
  a.answerer_user_id,

  -- Dates (DATE)
  a.creation_dt,
  a.last_activity_dt,

  -- Calendar helpers
  DATE_TRUNC(a.creation_dt, MONTH) AS creation_month,

  -- Date keys
  cd.date_key  AS creation_date_key,
  lad.date_key AS last_activity_date_key,

  -- Measures
  a.score,
  a.comment_count,

  -- Acceptance flag derived from questions table
  COALESCE(a.answer_id = q.accepted_answer_id, FALSE) AS is_accepted

FROM a
LEFT JOIN q   USING (question_id)
LEFT JOIN d cd  ON cd.date  = a.creation_dt
LEFT JOIN d lad ON lad.date = a.last_activity_dt
