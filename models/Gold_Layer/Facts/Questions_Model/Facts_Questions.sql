{{ config(materialized="table") }} 

WITH q AS (
  SELECT
    question_id,
    asker_user_id,
    accepted_answer_id,
    creation_dt,
    last_activity_dt,
    view_count,
    score,
    comment_count,
    favorite_count
  FROM {{ ref('Silver_Layer_Questions') }}
  WHERE question_id IS NOT NULL
),

answers_per_q AS (
  SELECT
    question_id,
    COUNT(DISTINCT answer_id) AS answer_count_calc
  FROM {{ ref('Silver_Layer_Answers') }}
  GROUP BY question_id
),

d AS (
  SELECT date_key, date
  FROM {{ ref('Dimensions_Date') }}
)

SELECT
  q.question_id,
  q.asker_user_id,
  q.accepted_answer_id,
  q.creation_dt,
  q.last_activity_dt,
  DATE_TRUNC(q.creation_dt, MONTH) AS creation_month,
  cd.date_key  AS creation_date_key,       
  lad.date_key AS last_activity_date_key,  
  GREATEST(
    COALESCE(apq.answer_count_calc, 0),
    CASE WHEN q.accepted_answer_id IS NOT NULL THEN 1 ELSE 0 END
  ) AS answer_count,

  q.view_count,
  q.score,
  q.favorite_count,
  q.comment_count,

  SAFE_CAST(
    GREATEST(
      COALESCE(apq.answer_count_calc, 0),
      CASE WHEN q.accepted_answer_id IS NOT NULL THEN 1 ELSE 0 END
    ) > 0 AS BOOL
  ) AS has_answers,

  SAFE_CAST(q.accepted_answer_id IS NOT NULL AS BOOL) AS has_accepted_answer
FROM q
LEFT JOIN answers_per_q apq USING (question_id)
LEFT JOIN d cd  ON cd.date  = q.creation_dt
LEFT JOIN d lad ON lad.date = q.last_activity_dt;
