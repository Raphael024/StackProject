{{ config(materialized="view") }}


with
    pairs as (
        select distinct q.question_id, lower(trim(tag)) as tag
        from {{ ref("Silver_Layer_Questions") }} q
        cross join unnest(coalesce(q.tags_array, array<string>[])) as tag
        where tag != ''
    )
select p.question_id, d.tag
from pairs p
join {{ ref("Dimensions_Tags") }} d using (tag)
