{% macro generate_schema_name(custom_schema_name, node) -%}
 
  {% if custom_schema_name is not none and custom_schema_name|trim != '' %}
    {{ ("dbt_" ~ custom_schema_name|lower|trim) }}
  {% else %}
    {{ target.schema }}
  {% endif %}
{%- endmacro %}
