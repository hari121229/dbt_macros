{%- macro dynamic_table(table_name) -%}

   SELECT 
   * 
   FROM {{ source('snowflake_sample_data', table_name ) }}

{%- endmacro %}
