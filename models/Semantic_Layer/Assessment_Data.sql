-- models/Semantic_Layer/Major_Report.sql
{{ config(materialized='view', alias='major_report') }}

-- 1) Base question facts (question grain)
WITH q AS (
  SELECT
    question_id,
    asker_user_id,
    accepted_answer_id,
    creation_date_key,
    last_activity_date_key,
    answer_count,
    view_count,
    score,
    favorite_count,
    has_answers,
    has_accepted_answer
  FROM {{ ref('facts_questions_vw') }}
),

-- 2) Question attributes (title, URL)
dq AS (
  SELECT
    question_id,
    title,
    question_url
  FROM {{ ref('dim_questions') }}
),

-- 3) Asker attributes (1:1)
asker AS (
  SELECT
    user_id        AS asker_user_id,
    display_name   AS asker_name,
    reputation     AS asker_reputation,
    country_guess  AS asker_country
  FROM {{ ref('dim_users') }}
),

-- 4) Accepted answer (max 1 row per question). Don't rely on creation_dt here.
accepted_ans AS (
  SELECT
    question_id,
    answer_id,
    answerer_user_id
  FROM (
    SELECT
      a.*,
      ROW_NUMBER() OVER (
        PARTITION BY question_id
        ORDER BY answer_id   -- deterministic; avoids referencing date columns
      ) AS rn
    FROM {{ ref('facts_answers_vw') }} a
    WHERE a.is_accepted
  )
  WHERE rn = 1
),

accepted_user AS (
  SELECT
    user_id       AS accepted_user_id,
    display_name  AS accepted_user_name,
    reputation    AS accepted_user_reputation
  FROM {{ ref('dim_users') }}
),

-- 5) Tags aggregated to question grain (no fan-out)
tags AS (
  SELECT
    b.question_id,
    ARRAY_AGG(DISTINCT t.tag_id ORDER BY t.tag_id) AS tag_ids,
    ARRAY_AGG(DISTINCT t.tag    ORDER BY t.tag)    AS tags_array,
    STRING_AGG(DISTINCT t.tag, '|' ORDER BY t.tag) AS tags_csv,
    COUNT(DISTINCT t.tag_id)                       AS tag_count
  FROM {{ ref('bridge_questions_tag_vw') }} b
  JOIN {{ ref('dim_tags') }} t USING (tag_id)
  GROUP BY b.question_id
),

-- 6) Date lookups: derive actual DATEs from the keys
cd AS (
  SELECT
    date_key,
    date AS creation_dt,
    EXTRACT(YEAR    FROM date) AS creation_year,
    EXTRACT(MONTH   FROM date) AS creation_month_num,
    EXTRACT(QUARTER FROM date) AS creation_quarter
  FROM {{ ref('dim_date') }}
),
lad AS (
  SELECT
    date_key,
    date AS last_activity_dt
  FROM {{ ref('dim_date') }}
)

-- 7) Final question-grain wide view (use cd/lad dates; never q.creation_dt)
SELECT
  q.question_id,

  -- Question attributes
  dq.title,
  dq.question_url,

  -- Asker
  q.asker_user_id,
  ak.asker_name,
  ak.asker_reputation,
  ak.asker_country,

  -- Dates (from dim_date lookups)
  cd.creation_dt,
  lad.last_activity_dt,
  DATE_TRUNC(cd.creation_dt, MONTH) AS creation_month,
  q.creation_date_key,
  q.last_activity_date_key,
  cd.creation_year,
  cd.creation_month_num,
  cd.creation_quarter,

  -- Measures & flags
  q.answer_count,   -- from COUNT DISTINCT in facts_questions_vw
  q.view_count,
  q.score,
  q.favorite_count,
  q.has_answers,
  q.has_accepted_answer,

  -- Accepted answer details (no fan-out)
  q.accepted_answer_id                             AS accepted_answer_id_from_q,
  aa.answer_id                                     AS accepted_answer_id_resolved,
  aa.answerer_user_id                              AS accepted_answerer_user_id,
  au.accepted_user_name,
  au.accepted_user_reputation,

  -- Tags
  t.tag_count,
  t.tags_array,
  t.tags_csv,
  t.tag_ids

FROM q
LEFT JOIN dq              USING (question_id)
LEFT JOIN asker      ak   USING (asker_user_id)
LEFT JOIN accepted_ans aa USING (question_id)
LEFT JOIN accepted_user au ON au.accepted_user_id = aa.answerer_user_id
LEFT JOIN tags        t   USING (question_id)
LEFT JOIN cd               ON cd.date_key  = q.creation_date_key
LEFT JOIN lad              ON lad.date_key = q.last_activity_date_key
