{{ config(schema="Semantic_Layer", materialized="view", alias="Question_Performance_Data") }}

WITH q AS (
  SELECT
    fq.question_id,
    fq.asker_user_id,
    fq.accepted_answer_id,
    fq.creation_date_key,
    fq.answer_count,
    fq.view_count,
    fq.score,
    fq.favorite_count,
    fq.comment_count,
    fq.has_answers,
    fq.has_accepted_answer,
    dd.date                                   AS creation_dt,
    DATE_TRUNC(dd.date, MONTH)                AS creation_month
  FROM {{ ref('Facts_Questions') }} fq
  LEFT JOIN {{ ref('Dimensions_Date') }} dd
    ON dd.date_key = fq.creation_date_key
  WHERE fq.question_id IS NOT NULL
),

answers_in_scope AS (
  SELECT
    q.creation_month,
    a.answer_id,
    a.answerer_user_id
  FROM {{ ref('Silver_Layer_Answers') }} a
  JOIN q USING (question_id)
),

tags_by_q AS (
  SELECT
    b.question_id,
    dt.tag
  FROM {{ ref('Bridge_Questions_Tag') }} b
  JOIN {{ ref('Dimensions_Tags') }} dt
    ON dt.tag_id = b.tag_id
)

SELECT
  -- Grain
  q.creation_month,
  EXTRACT(YEAR    FROM q.creation_month) AS creation_year,
  EXTRACT(MONTH   FROM q.creation_month) AS creation_month_num,
  EXTRACT(QUARTER FROM q.creation_month) AS creation_quarter,
  MIN(q.question_id)                     AS sample_question_id,
  COUNT(DISTINCT q.question_id)          AS distinct_count_of_question_id,
  COUNT(DISTINCT q.asker_user_id)        AS distinct_count_of_asker_user_id,
  COUNT(DISTINCT q.accepted_answer_id)   AS distinct_count_of_accepted_answer_id,
  COUNT(DISTINCT a.answer_id)            AS distinct_count_of_answerer_id,
  COUNT(DISTINCT a.answerer_user_id)     AS distinct_count_of_answerer_user_id,
  SUM(q.answer_count)                    AS sum_of_answer_count,
  SUM(q.view_count)                      AS sum_of_view_count,
  SUM(q.favorite_count)                  AS sum_of_favorite_count,
  SUM(q.comment_count)                   AS comment_count_summed,
  SUM(CASE WHEN q.has_answers THEN 1 ELSE 0 END)          AS questions_with_answers,
  SUM(CASE WHEN q.has_accepted_answer THEN 1 ELSE 0 END)  AS questions_with_accepted,
  SAFE_DIVIDE(
    SUM(CASE WHEN q.has_answers THEN 1 ELSE 0 END),
    NULLIF(COUNT(DISTINCT q.question_id),0)
  ) AS has_answers_rate,
  SAFE_DIVIDE(
    SUM(CASE WHEN q.has_accepted_answer THEN 1 ELSE 0 END),
    NULLIF(COUNT(DISTINCT q.question_id),0)
  ) AS accepted_rate,
  ARRAY_AGG(DISTINCT tq.tag ORDER BY tq.tag)                    AS tags_array,
  STRING_AGG(DISTINCT tq.tag, '|' ORDER BY tq.tag)              AS tags_csv
FROM q
LEFT JOIN answers_in_scope a
  ON a.creation_month = q.creation_month
LEFT JOIN tags_by_q tq
  ON tq.question_id = q.question_id
GROUP BY
  q.creation_month,
  creation_year,
  creation_month_num,
  creation_quarter
;
