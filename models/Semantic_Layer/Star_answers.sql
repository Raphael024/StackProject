{{ config(materialized='view') }}

-- Tags per question to decorate answers with the question’s topics
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
  -- fact (grain: 1 row per answer)
  fa.answer_id,
  fa.question_id,
  fa.creation_date,
  fa.creation_month,
  fa.last_activity_date,

  -- measures
  fa.score,
  fa.comment_count,
  fa.is_accepted,

  -- dims
  dq.title          AS question_title,
  dq.question_url,

  ua.user_id        AS answerer_user_id,
  ua.display_name   AS answerer_display_name,
  ua.reputation     AS answerer_reputation,
  ua.country_guess  AS answerer_country,
  ua.tenure_days    AS answerer_tenure_days,
  ua.is_active_90d  AS answerer_is_active_90d,

  -- date dims (denormalized)
  dca.date          AS creation_dim_date,
  dca.year          AS creation_year,
  dca.month         AS creation_month_num,
  dla.date          AS last_act_dim_date,
  dla.year          AS last_act_year,
  dla.month         AS last_act_month_num,

  -- tags of the parent question
  t.tags_array,
  t.tags_csv

FROM {{ ref('facts_answers') }} fa
LEFT JOIN {{ ref('dim_questions') }} dq   ON dq.question_id = fa.question_id
LEFT JOIN {{ ref('dim_users') }} ua       ON ua.user_id     = fa.answerer_user_id
LEFT JOIN {{ ref('dim_date') }} dca       ON dca.date_key   = fa.creation_date_key
LEFT JOIN {{ ref('dim_date') }} dla       ON dla.date_key   = fa.last_activity_date_key
LEFT JOIN tags_per_q t                    ON t.question_id  = fa.question_id
