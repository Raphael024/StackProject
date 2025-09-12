{{ config(materialized="view") }}

with
  pairs as (
    select distinct
      q.question_id,
      lower(trim(tag)) as tag
    from {{ ref("Silver_Layer_Questions") }} q,
    unnest(coalesce(q.tags_array, ARRAY<STRING>[])) as tag
    where tag is not null and trim(tag) <> ''
  )

select distinct
  p.question_id,
  d.tag_id
from pairs p
join {{ ref("Dimensions_Tags") }} d
  on d.tag = p.tag
