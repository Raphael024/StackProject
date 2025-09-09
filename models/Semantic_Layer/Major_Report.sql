-- models/Semantic_Layer/sem_questions_with_answer_stats_vw.sql
{{ config(materialized='view') }}

with
-- 1) Base question facts (one row per question)
q as (
  select
    question_id,
    asker_user_id,
    accepted_answer_id,
    creation_date,
    last_activity_date,
    creation_month,
    creation_date_key,
    last_activity_date_key,
    answer_count,
    view_count,
    score                as question_score,
    favorite_count,
    has_answers,
    has_accepted_answer
  from {{ ref('facts_questions_vw') }}
),

-- 2) Answer metrics aggregated up to the QUESTION level (prevents duplication)
ans as (
  select
    question_id,
    count(*)                                 as answers_total,
    countif(is_accepted)                     as answers_accepted,
    safe_divide(countif(is_accepted), count(*)) as acceptance_rate,   -- 0..1
    avg(score)                               as avg_answer_score,
    max(last_activity_date)                  as last_answer_activity_date
  from {{ ref('facts_answers_vw') }}
  group by question_id
),

-- 3) Question dimension (titles/URLs)
dq as (
  select question_id, title as question_title, question_url
  from {{ ref('dim_questions') }}
),

-- 4) Users dimension (asker)
du as (
  select user_id, display_name, reputation, location, country_guess, tenure_days, is_active_90d
  from {{ ref('dim_users') }}
),

-- 5) Tags (rolled up for filters)
q_tags as (
  select
    b.question_id,
    array_agg(t.tag order by t.tag)              as tags_array,
    string_agg(t.tag, ', ' order by t.tag)       as tags_csv
  from {{ ref('bridge_questions_tag_vw') }} b
  join {{ ref('dim_tags') }} t using (tag_id)
  group by b.question_id
)

select
  -- Grain: one row per question
  q.question_id,

  -- Question attributes
  q.asker_user_id,
  dq.question_title,
  dq.question_url,

  q.creation_date,
  q.creation_month,
  q.last_activity_date,
  q.creation_date_key,
  q.last_activity_date_key,

  q.answer_count,
  q.view_count,
  q.question_score,
  q.favorite_count,
  q.has_answers,
  q.has_accepted_answer,

  -- Tags
  qt.tags_array,
  qt.tags_csv,

  -- Asker attributes
  ua.display_name     as asker_display_name,
  ua.reputation       as asker_reputation,
  ua.location         as asker_location,
  ua.country_guess    as asker_country,
  ua.tenure_days      as asker_tenure_days,
  ua.is_active_90d    as asker_is_active_90d,

  -- Aggregated answer quality metrics (safe to join 1:1)
  coalesce(ans.answers_total, 0)                 as answers_total,
  coalesce(ans.answers_accepted, 0)              as answers_accepted,
  coalesce(ans.acceptance_rate, 0.0)             as acceptance_rate,       -- 0..1
  coalesce(ans.avg_answer_score, 0.0)            as avg_answer_score,
  ans.last_answer_activity_date                  as last_answer_activity_date,

  -- Convenience: popularity x unanswered signal (optional)
  case
    when q.has_accepted_answer then 0
    else q.view_count
  end as unanswered_popularity_score

from q
left join ans using (question_id)              -- 1:1 after aggregation
left join dq using (question_id)
left join q_tags qt using (question_id)
left join du ua on ua.user_id = q.asker_user_id
