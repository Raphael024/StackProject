{{ config(materialized='view') }}

-- Tags per question to avoid row explosion
WITH tags_per_q AS (
  SELECT
    b.question_id,
    ARRAY_AGG(dt.tag ORDER BY dt.tag) AS tags_array,
    STRING_AGG(dt.tag, ', ' ORDER BY dt.tag) AS tags_csv
  FROM {{ ref('bridge_questions_tag') }} b
  JOIN {{ ref('dim_tags') }} dt USING (tag_id)
  GROUP BY b.question_id
)

SELECT
  -- fact (grain: 1 row per question)
  fq.question_id,
  fq.creation_date,
  fq.creation_month,
  fq.last_activity_date,

  -- measures
  fq.answer_count,
  fq.view_count,
  fq.score,
  fq.favorite_count,
  fq.has_answers,
  fq.has_accepted_answer,

  -- dims
  dq.title AS question_title,
  dq.question_url,

  u.user_id        AS asker_user_id,
  u.display_name   AS asker_display_name,
  u.reputation     AS asker_reputation,
  u.country_guess  AS asker_country,
  u.tenure_days    AS asker_tenure_days,
  u.is_active_90d  AS asker_is_active_90d,

  -- date dims (denormalized for convenience)
  dc.date          AS creation_dim_date,
  dc.year          AS creation_year,
  dc.month         AS creation_month_num,
  dc.quarter       AS creation_quarter,

  dla.date         AS last_act_dim_date,
  dla.year         AS last_act_year,
  dla.month        AS last_act_month_num,

  -- tags (array + csv)
  t.tags_array,
  t.tags_csv

FROM {{ ref('facts_questions') }} fq
LEFT JOIN {{ ref('dim_questions') }} dq   USING (question_id)
LEFT JOIN {{ ref('dim_users') }} u        ON u.user_id = fq.asker_user_id
LEFT JOIN {{ ref('dim_date') }} dc        ON dc.date_key  = fq.creation_date_key
LEFT JOIN {{ ref('dim_date') }} dla       ON dla.date_key = fq.last_activity_date_key
LEFT JOIN tags_per_q t                    USING (question_id)
