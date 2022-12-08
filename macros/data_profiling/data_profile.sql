
-- Create the schema in snowflake if not exists
{%- macro create_new_schema(db_name, schema_name) -%}
    CREATE SCHEMA IF NOT EXISTS {{ db_name }}.{{ schema_name }}
{%- endmacro -%}

{% macro data_profiling(target_database, target_schema, exclude_tables, include_tables, destination_database, destination_schema, destination_table) %}

{% if (flags.WHICH).upper() == 'RUN' %}
    -- Configure the destination details
    {%- set snowflake_database   = destination_database-%}
    {%- set snowflake_schema     = destination_schema -%}
    {%- set snowflake_tables     = [destination_table] -%}

    {%- set source_details  =  [[ target_database, target_schema, exclude_tables, include_tables ]] -%}

    {% set get_current_timestamp %}
        SELECT {{ current_timestamp_utc() }} AS utc_time_zone
    {% endset %}
    {% if execute %}
        {% set profiled_at = run_query(get_current_timestamp).columns[0].values()[0] %}
    {% endif %}
    {%- set schema_create -%}
        {{ data_quality.create_new_schema(snowflake_database, snowflake_schema) }}
    {%- endset -%}
    {% do run_query(schema_create) %}
    -- Iterate through the layer
    {%- for snowflake_table in snowflake_tables -%}
        -- Create the table in snowflake if not exists
        {%- set create_table -%}
            {{ data_quality.create_data_profiling_table(snowflake_database, snowflake_schema, snowflake_table) }}
        {%- endset -%}
        {% do run_query(create_table) %}
        -- Read the table names from information schema for that particular layer
        {%- set read_information_schema_datas -%}
            {{ data_quality.read_information_schema(source_details[loop.index-1][0], source_details[loop.index-1][1], source_details[loop.index-1][2], source_details[loop.index-1][3]) }}
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
                                    {{ data_quality.do_data_profiling(information_schema_data, source_table_name, chunk_column, profiled_at) }}
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
                                {{ data_quality.do_data_profiling(information_schema_data, source_table_name, chunk_column, profiled_at) }}
                                {% if not loop.last %} UNION ALL {% endif %}
                            {%- endfor -%}
                        )
                {%- endset -%}
                {% do run_query(insert_rows) %}
                {%- do chunk_columns.clear() -%}
            {%- endif -%}
        {%- endfor %}
    {%- endfor %}
{% endif %}

    SELECT 'TEMP_STORAGE' AS temp_column
{% endmacro %}


-- To check the column is numeric or not
{%- macro is_numeric_dtype(dtype) -%}
    {% set is_numeric = dtype.startswith("int") or dtype.startswith("float") or "numeric" in dtype or "number" in dtype or "double" in dtype %}
    {% do return(is_numeric) %}
{%- endmacro -%}
-- To check the column is data/time or not
{%- macro is_date_or_time_dtype(dtype) -%}
    {% set is_date_or_time = dtype.startswith("timestamp") or dtype.startswith("date") %}
    {% do return(is_date_or_time) %}
{%- endmacro -%}
-- Create table if not exists
{%- macro create_data_profiling_table(db_name, schema_name, table_name) -%}
    CREATE TABLE IF NOT EXISTS {{ db_name }}.{{ schema_name }}.{{ table_name }}(
        database                    VARCHAR(100)
        , schema                    VARCHAR(100)
        , table_name                VARCHAR(100)
        , column_name               VARCHAR(500)
        , data_type                 VARCHAR(100)
        , row_count                 NUMBER(38,0)
        , not_null_count            NUMBER(38,0)
        , null_count                NUMBER(38,0)
        , not_null_percentage       NUMBER(38,2)
        , null_percentage           NUMBER(38,2)
        , distinct_count            NUMBER(38,0)
        , distinct_percent          NUMBER(38,2)
        , is_unique                 BOOLEAN
        , min                       VARCHAR(250)
        , max                       VARCHAR(250)
        , avg                       NUMBER(38,2)
        , profiled_at               TIMESTAMP_NTZ(9)
    )
{%- endmacro -%}
-- Read the data from information schema based on the parameters
{%- macro read_information_schema(db_name, profiling_schemas, exclude_tables=[],include_tables=[]) -%}
    SELECT
        table_catalog           AS table_database
        , table_schema
        , table_name
    FROM {{ db_name }}.INFORMATION_SCHEMA.TABLES
    WHERE
        table_schema IN ( {%- for profiling_schema in profiling_schemas -%}
                                '{{ profiling_schema.upper()}}'
                                {%- if not loop.last -%} , {% endif -%}
                            {%- endfor -%} )
        {% if exclude_tables != [] %}
            AND table_name NOT IN ( {%- for exclude_table in exclude_tables -%}
                                    '{{ exclude_table.upper() }}'
                                    {%- if not loop.last -%} , {% endif -%}
                                {%- endfor -%} )
        
         {% elif include_tables != [] %}
            AND table_name IN ( {%- for include_table in include_tables -%}
                                    '{{ include_table.upper() }}'
                                    {%- if not loop.last -%} , {% endif -%}
                                {%- endfor -%} )
        {% else %}

            AND 1 = 1
        
        {% endif %}

    ORDER BY table_schema, table_name
{%- endmacro -%}
-- Get the profiling details for the column
{%- macro do_data_profiling(information_schema_data, source_table_name, chunk_column, current_date_and_time) -%}
    SELECT
        '{{ information_schema_data[0] }}'      AS database
        , '{{ information_schema_data[1] }}'    AS schema
        , '{{ information_schema_data[2] }}'    AS table_name
        , '{{ chunk_column["column"] }}'        AS column_name
        , '{{ chunk_column["dtype"] }}'         AS data_type
        , CAST(COUNT(*) AS NUMERIC)             AS row_count
        , SUM(CASE 
                WHEN IFF({{ adapter.quote(chunk_column["column"]) }}::VARCHAR = '', NULL, {{ adapter.quote(chunk_column["column"]) }}) IS NULL
                    THEN 0
                ELSE 1
            END)     AS not_null_count
        , SUM(CASE 
                WHEN IFF({{ adapter.quote(chunk_column["column"]) }}::VARCHAR = '', NULL, {{ adapter.quote(chunk_column["column"]) }}) IS NULL
                    THEN 1
                ELSE 0
            END)    AS null_count
        , ROUND((not_null_count / CAST(COUNT(*) AS NUMERIC)) * 100, 2)      AS not_null_percentage
        , ROUND((null_count / CAST(COUNT(*) AS NUMERIC)) * 100, 2)          AS null_percentage
        , COUNT(DISTINCT IFF({{ adapter.quote(chunk_column["column"]) }}::VARCHAR = '', NULL, {{ adapter.quote(chunk_column["column"]) }}))                                                      AS distinct_count
        , ROUND((COUNT(DISTINCT IFF({{ adapter.quote(chunk_column["column"]) }}::VARCHAR = '', NULL, {{ adapter.quote(chunk_column["column"]) }})) / CAST(COUNT(*) AS NUMERIC)) * 100, 2)        AS distinct_percent
        , COUNT(DISTINCT IFF({{ adapter.quote(chunk_column["column"]) }}::VARCHAR = '', NULL, {{ adapter.quote(chunk_column["column"]) }})) = COUNT(*)                                           AS is_unique
        , {% if data_quality.is_numeric_dtype((chunk_column["dtype"]).lower()) or data_quality.is_date_or_time_dtype((chunk_column["dtype"]).lower()) %}
            CAST(MIN({{ adapter.quote(chunk_column["column"]) }}) AS VARCHAR)
        {% else %}
            NULL
        {% endif %}   AS min
        , {% if data_quality.is_numeric_dtype((chunk_column["dtype"]).lower()) or data_quality.is_date_or_time_dtype((chunk_column["dtype"]).lower()) %}
            CAST(MAX({{ adapter.quote(chunk_column["column"]) }}) AS VARCHAR)
        {% else %}
            NULL
        {% endif %}   AS max
        , {% if data_quality.is_numeric_dtype((chunk_column["dtype"]).lower()) %}
            ROUND(AVG({{ adapter.quote(chunk_column["column"]) }}), 2)
        {% else %}
            CAST(NULL AS NUMERIC)
        {% endif %}   AS avg
        , CAST('{{ current_date_and_time }}' AS TIMESTAMP_NTZ)    AS profiled_at
    FROM {{ source_table_name }}
{%- endmacro -%}

