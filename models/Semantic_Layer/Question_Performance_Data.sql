{{ config(schema="Semantic_Layer", materialized="view", alias="Question_Performance_Data") }}

WITH fq AS (
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
    has_accepted_answer,
    comment_count
  FROM {{ ref('Facts_Questions') }}
),
aa AS (
  SELECT question_id, answer_id, answerer_user_id
  FROM (
    SELECT a.*, ROW_NUMBER() OVER (PARTITION BY question_id ORDER BY answer_id) AS rn
    FROM {{ ref('Silver_Layer_Answers') }} a
    WHERE a.is_accepted
  )
  WHERE rn = 1
),
qtags AS (
  SELECT b.question_id, t.tag
  FROM {{ ref('Bridge_Questions_Tag') }} b
  JOIN {{ ref('Dimensions_Tags') }} t USING (tag_id)   -- switch to USING(tag) if your bridge stores text
),
cd AS (
  SELECT date_key, date AS creation_dt,
         EXTRACT(YEAR FROM date)    AS creation_year,
         EXTRACT(MONTH FROM date)   AS creation_month_num,
         EXTRACT(QUARTER FROM date) AS creation_quarter
  FROM {{ ref('Dimensions_Date') }}
)

SELECT
  DATE_TRUNC(cd.creation_dt, MONTH) AS creation_month,
  cd.creation_year,
  cd.creation_month_num,
  cd.creation_quarter,
  MIN(fq.question_id) AS sample_question_id,
  COUNT(DISTINCT fq.question_id)        AS distinct_count_of_question_id,
  COUNT(DISTINCT fq.asker_user_id)      AS distinct_count_of_asker_user_id,
  COUNT(DISTINCT fq.accepted_answer_id) AS distinct_count_of_accepted_answer_id,
  COUNT(DISTINCT aa.answer_id)          AS distinct_count_of_answerer_id,
  COUNT(DISTINCT aa.answerer_user_id)   AS distinct_count_of_answerer_user_id,
  SUM(fq.answer_count)    AS sum_of_answer_count,
  SUM(fq.view_count)      AS sum_of_view_count,
  SUM(fq.favorite_count)  AS sum_of_favorite_count,
  SUM(fq.comment_count)   AS comment_count_summed,
  SUM(CAST(fq.has_answers AS INT64))          AS questions_with_answers,
  SUM(CAST(fq.has_accepted_answer AS INT64))  AS questions_with_accepted,
  SAFE_DIVIDE(SUM(CAST(fq.has_answers AS FLOAT64)), COUNT(1))         AS has_answers_rate,
  SAFE_DIVIDE(SUM(CAST(fq.has_accepted_answer AS FLOAT64)), COUNT(1)) AS accepted_rate,
  ARRAY_AGG(DISTINCT qt.tag ORDER BY qt.tag)       AS tags_array,
  STRING_AGG(DISTINCT qt.tag, '|' ORDER BY qt.tag) AS tags_csv
FROM fq
LEFT JOIN aa        USING (question_id)
LEFT JOIN cd        ON cd.date_key = fq.creation_date_key
LEFT JOIN qtags qt  ON qt.question_id = fq.question_id
GROUP BY
  creation_month, cd.creation_year, cd.creation_month_num, cd.creation_quarter;
