{{ config(schema="Semantic_Layer", materialized="view", alias="Question_Performance_Data") }}

WITH q AS (
  SELECT
    fq.creation_dt,
    dq.title              AS question_title,
    du.country_guess      AS asker_country,
    du.reputation         AS asker_reputation,
    fq.last_activity_dt,
    fq.answer_count,
    fq.view_count,
    fq.favorite_count,
    fq.comment_count
  FROM {{ ref('Facts_Questions') }} fq
  LEFT JOIN {{ ref('Dimensions_Questions') }} dq
    ON dq.question_id = fq.question_id
  LEFT JOIN {{ ref('Dimensions_Users') }} du
    ON du.user_id = fq.asker_user_id
  WHERE fq.question_id IS NOT NULL
)

SELECT
  q.creation_dt,
  q.question_title,
  q.asker_country,
  q.asker_reputation,
  q.last_activity_dt,
  SUM(q.answer_count)    AS sum_of_answer_count,
  SUM(q.view_count)      AS sum_of_view_count,
  SUM(q.favorite_count)  AS sum_of_favorite_count,
  SUM(q.comment_count)   AS comment_count_summed
FROM q
GROUP BY
  q.creation_dt,
  q.question_title,
  q.asker_country,
  q.asker_reputation,
  q.last_activity_dt
