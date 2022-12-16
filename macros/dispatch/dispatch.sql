{% macro current_timestamp_utc() -%}
  {{ return(adapter.dispatch('current_timestamp_utc')()) }}
{%- endmacro %}


{% macro snowflake__current_timestamp_utc() -%}
CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) 
{%- endmacro %}

{% macro postgres__current_timestamp_utc() -%}
CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) 
{%- endmacro %}
