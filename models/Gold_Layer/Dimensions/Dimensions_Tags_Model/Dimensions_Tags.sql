{{ config(materialized="view", cluster_by=["tag_id"]) }}

with
  tags_from_questions as (
    select distinct lower(trim(tag)) as tag
    from {{ ref("Silver_Layer_Questions") }} q,
    unnest(coalesce(q.tags_array, array<string>[])) as tag
    where tag is not null and trim(tag) <> ''
  ),

  tags_attrs as (
    select
      lower(trim(t.tag)) as tag,
      t.tag_count_raw,              -- already INT64 in Silver layer
      t.excerpt_post_id,
      t.wiki_post_id
    from {{ ref("Silver_Layer_Tags") }} t
  )

select
  -- deterministic surrogate key (SK) for conformance
  to_hex(md5(tq.tag))                         as tag_id,
  -- business key (BK)
  tq.tag                                      as tag,
  -- attrs (nullable if not present in Silver_Layer_Tags)
  ta.tag_count_raw,
  ta.excerpt_post_id,
  ta.wiki_post_id,
  -- hygiene flags
  regexp_contains(tq.tag, r'[^a-z0-9\-\+\#\.]') as has_illegal_chars,
  coalesce(ta.tag_count_raw, 0) = 0            as is_zero_count
from tags_from_questions tq
left join tags_attrs ta using (tag);

