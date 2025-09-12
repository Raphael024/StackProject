{{ config(schema="Semantic_Layer", materialized="view", alias="Question_Performance_Data") }}

WITH q AS (
  SELECT
    fq.question_id,
    dq.title                                          AS question_title,
    du.country_guess                                  AS asker_country,
    SAFE_CAST(du.reputation AS INT64)                 AS asker_reputation,
    c.date                                            AS creation_dt,
    l.date                                            AS last_activity_dt,
    fq.answer_count,
    fq.view_count,
    fq.favorite_count,
    fq.comment_count
  FROM {{ ref('Facts_Questions') }} fq
  LEFT JOIN {{ ref('Dimensions_Questions') }} dq USING (question_id)
  LEFT JOIN {{ ref('Dimensions_Users') }}     du ON du.user_id = fq.asker_user_id
  LEFT JOIN {{ ref('Dimensions_Date') }}      c  ON c.date_key = fq.creation_date_key
  LEFT JOIN {{ ref('Dimensions_Date') }}      l  ON l.date_key = fq.last_activity_date_key
  WHERE fq.question_id IS NOT NULL
)

SELECT
  q.creation_dt,
  q.question_title,
  q.asker_country,
  q.asker_reputation,
  q.last_activity_dt,
  SUM(q.answer_count)   AS sum_of_answer_count,
  SUM(q.view_count)     AS sum_of_view_count,
  SUM(q.favorite_count) AS sum_of_favorite_count,
  SUM(q.comment_count)  AS comment_count_summed
FROM q
GROUP BY
  q.creation_dt,
  q.question_title,
  q.asker_country,
  q.asker_reputation,
  q.last_activity_dt