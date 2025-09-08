{{ config(materialized='view') }}

WITH bqt AS (
  -- unique question ↔ tag pairs from your built bridge
  SELECT DISTINCT question_id, tag_id
  FROM {{ ref('bridge_questions_tag') }}
),
tags AS (
  -- tag text for display
  SELECT tag_id, tag
  FROM {{ ref('dim_tags') }}
),
first_answer AS (
  -- first answer date per question (date grain)
  SELECT
    question_id,
    MIN(creation_date) AS first_answer_date
  FROM {{ ref('facts_answers') }}
  GROUP BY question_id
),
q_base AS (
  -- one row per (question, tag_id) with question attributes
  SELECT
    b.tag_id,
    t.tag,
    fq.question_id,
    fq.creation_date,
    fq.has_answers,
    fq.has_accepted_answer
  FROM {{ ref('facts_questions') }} fq
  JOIN bqt b USING (question_id)
  JOIN tags t USING (tag_id)
),
a_base AS (
  -- answers mapped to the same tag_id via the bridge
  SELECT
    b.tag_id,
    fa.question_id,
    fa.answer_id,
    fa.is_accepted,
    fa.creation_date
  FROM {{ ref('facts_answers') }} fa
  JOIN bqt b USING (question_id)
)

SELECT
  qb.tag_id,
  qb.tag,
  qb.creation_date,
  COUNT(DISTINCT qb.question_id) AS questions,
  COUNT(DISTINCT a_base.answer_id) AS answers,
  COUNT(DISTINCT IF(qb.has_accepted_answer, qb.question_id, NULL)) AS questions_with_accepted_answer,
  COUNT(DISTINCT IF(qb.has_answers,         qb.question_id, NULL)) AS questions_with_any_answer,
  SAFE_DIVIDE(
    COUNT(DISTINCT IF(qb.has_answers, qb.question_id, NULL)),
    COUNT(DISTINCT qb.question_id)
  ) AS answer_rate,
  1 - SAFE_DIVIDE(
    COUNT(DISTINCT IF(qb.has_answers, qb.question_id, NULL)),
    COUNT(DISTINCT qb.question_id)
  ) AS unanswered_rate,
  AVG(DATE_DIFF(fa.first_answer_date, qb.creation_date, DAY)) AS avg_days_to_first_answer
FROM q_base qb
LEFT JOIN first_answer fa USING (question_id)
LEFT JOIN a_base
  ON a_base.question_id = qb.question_id
 AND a_base.tag_id      = qb.tag_id
GROUP BY 1,2,3
