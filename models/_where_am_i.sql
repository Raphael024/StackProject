{{ config(materialized='table') }}
select '{{ target.database }}' as project, '{{ target.schema }}' as dataset
