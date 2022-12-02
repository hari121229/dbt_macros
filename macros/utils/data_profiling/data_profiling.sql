{%- macro create_new_schema(db_name, schema_name) -%}
    CREATE SCHEMA IF NOT EXISTS {{ db_name }}.{{ schema_name }}
{%- endmacro -%}

{% macro data_profiling(target_database,target_schema,exclude_columns) %}

    -- Configure the destination detailsss
    {%- set snowflake_database   = 'dbt_learning' -%}
    {%- set snowflake_schema     = 'orderdetils' -%}
    {%- set snowflake_tables     = target_database -%}

    {%- set source_details  =  [[ target_database[0], target_schema, exclude_columns]] -%}

    {% set get_current_timestamp %}
        SELECT CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS utc_time_zone
    {% endset %}
    {% if execute %}
        {% set profiled_at = run_query(get_current_timestamp).columns[0].values()[0] %}
    {% endif %}
    {%- set schema_create -%}
        {{ create_new_schema(snowflake_database, snowflake_schema) }}
    {%- endset -%}
    {% do run_query(schema_create) %}
    -- Iterate through the layer
    {%- for snowflake_table in snowflake_tables -%}
        -- Create the table in snowflake if not exists
        {%- set create_table -%}
            {{ create_data_profiling_table(snowflake_database, snowflake_schema, snowflake_table) }}
        {%- endset -%}
        {% do run_query(create_table) %}
        -- Read the table names from information schema for that particular layer
        {%- set read_information_schema_datas -%}
            {{ read_information_schema(source_details[loop.index-1][0], source_details[loop.index-1][1], source_details[loop.index-1][2]) }}
        {%- endset -%}
        {% set information_schema_datas = run_query(read_information_schema_datas) %}
        -- This loop is used to itetrate the tables in layer
        {%- for information_schema_data in information_schema_datas -%}
            {%- set source_table_name = information_schema_data[0] + '.' + information_schema_data[1] + '.' + information_schema_data[2] -%}
            {%- set source_columns    = adapter.get_columns_in_relation(source_table_name) | list -%}
            {%- set chunk_columns     = [] -%}
            -- This loop is used to iterate the columns inside the table
            {%- for source_column in source_columns -%}
                {%- do chunk_columns.append(source_column) -%}
                {%- if (chunk_columns | length) == 100 -%}
                    {%- set insert_rows -%}
                        INSERT INTO {{ snowflake_database }}.{{ snowflake_schema }}.{{ snowflake_table }} (
                                {%- for chunk_column in chunk_columns -%}
                                    {{ do_data_profiling(information_schema_data, source_table_name, chunk_column, profiled_at) }}
                                    {% if not loop.last %} UNION ALL {% endif %}
                                {%- endfor -%}
                            )
                    {%- endset -%}
                    {% do run_query(insert_rows) %}
                    {%- do chunk_columns.clear() -%}
                {%- endif -%}
            {%- endfor -%}
            -- This condition iterate the columns if any of them are missed in above condition
            {%- if (chunk_columns | length) != 0 -%}
                {%- set insert_rows -%}
                    INSERT INTO {{ snowflake_database }}.{{ snowflake_schema }}.{{ snowflake_table }} (
                            {%- for chunk_column in chunk_columns -%}
                                {{ do_data_profiling(information_schema_data, source_table_name, chunk_column, profiled_at) }}
                                {% if not loop.last %} UNION ALL {% endif %}
                            {%- endfor -%}
                        )
                {%- endset -%}
                {% do run_query(insert_rows) %}
                {%- do chunk_columns.clear() -%}
            {%- endif -%}
        {%- endfor %}
    {%- endfor %}

    SELECT 'TEMP_STORAGE' AS temp_column
{% endmacro %}