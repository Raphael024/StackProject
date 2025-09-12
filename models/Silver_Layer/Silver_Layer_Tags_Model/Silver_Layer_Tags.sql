{{ config(materialized="view") }}
with
    base as (select * from {{ source("DBT_RAW", "v_tags") }} where tag is not null),
    clean as (
        select
            (select as struct b.*) as raw_record,
            lower(trim(b.tag)) as tag,
            coalesce(safe_cast(b.tag_count as int64), 0) as tag_count_raw,
            safe_cast(b.excerpt_post_id as int64) as excerpt_post_id,
            safe_cast(b.wiki_post_id as int64) as wiki_post_id
        from base b
        where trim(b.tag) is not null and trim(b.tag) <> ''
    ),
    dedup as (
        select *
        from clean
        qualify
            row_number() over (
                partition by tag
                order by tag_count_raw desc, excerpt_post_id desc, wiki_post_id desc
            )
            = 1
    )
select *
from dedup
