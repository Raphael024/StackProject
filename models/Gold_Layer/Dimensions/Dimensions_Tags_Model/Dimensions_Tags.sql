{{ config(materialized="view", cluster_by=["tag_id"]) }}

with
    tags_from_questions as (
        select distinct lower(trim(tag)) as tag
        from {{ ref("Silver_Layer_Questions") }} q
        cross join unnest(coalesce(q.tags_array, array<string>[])) as tag
        where tag is not null and trim(tag) <> ''
    ),
    tags_attrs as (
        select
            lower(trim(t.tag)) as tag,
            safe_cast(t.tag_count_raw as int64) as tag_count_raw,
            t.excerpt_post_id,
            t.wiki_post_id
        from {{ ref("Silver_Layer_Tags") }} t
    )

select

    cast(abs(farm_fingerprint(tq.tag)) as int64) as tags_id,
    tq.tag,
    ta.tag_count_raw,
    ta.excerpt_post_id,
    ta.wiki_post_id,
    regexp_contains(tq.tag, r'[^a-z0-9\-\+\#\.]') as has_illegal_chars,
    ifnull(ta.tag_count_raw, 0) = 0 as is_zero_count
from tags_from_questions tq
left join tags_attrs ta using (tag)
