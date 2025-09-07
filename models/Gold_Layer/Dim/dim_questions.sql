{{ config(materialized='table') }}

SELECT
  q.question_id,            -- PK
  q.title,
  q.question_url,

  -- Body-derived attributes (require Silver patch; remove if body not available)
  q.body_text,
  q.body_char_len,
  q.code_block_count,
  q.link_count
FROM {{ ref('stg_so_questions') }} q
