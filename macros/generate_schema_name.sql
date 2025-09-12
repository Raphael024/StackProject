{% macro generate_schema_name(custom_schema_name, node) -%}
  {{ ("dbt_" ~ (custom_schema_name | lower)) }}
{%- endmacro %}
