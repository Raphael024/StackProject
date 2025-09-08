{{ config(materialized='view') }}

SELECT
  fq.question_id,
  dt.tag_id,
  dt.tag,
  fq.creation_date,
  fq.creation_month,
  fq.answer_count,
  fq.view_count,
  fq.score,
  fq.favorite_count,
  fq.has_answers,
  fq.has_accepted_answer,
  dq.title  AS question_title,
  dq.question_url
FROM {{ ref('facts_questions') }} fq
JOIN {{ ref('bridge_questions_tag') }} b   USING (question_id)
JOIN {{ ref('dim_tags') }} dt              USING (tag_id)
LEFT JOIN {{ ref('dim_questions') }} dq    USING (question_id)
