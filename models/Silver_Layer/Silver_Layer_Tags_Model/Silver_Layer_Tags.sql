{{ config(materialized="view") }}

with t as (select * from {{ source("DBT_RAW", "v_tags") }} where tag is not null)
select

    (select as struct t.*) as raw_record,

    lower(trim(t.tag)) as tag,
    safe_cast(t.tag_count as int64) as tag_count_raw,
    safe_cast(t.excerpt_post_id as int64) as excerpt_post_id,
    safe_cast(t.wiki_post_id as int64) as wiki_post_id
from t
