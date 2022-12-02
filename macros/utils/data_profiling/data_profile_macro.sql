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
{%- macro read_information_schema(db_name, profiling_schemas, exclude_tables=[]) -%}
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
        , {% if is_numeric_dtype((chunk_column["dtype"]).lower()) or is_date_or_time_dtype((chunk_column["dtype"]).lower()) %}
            CAST(MIN({{ adapter.quote(chunk_column["column"]) }}) AS VARCHAR)
        {% else %}
            NULL
        {% endif %}   AS min
        , {% if is_numeric_dtype((chunk_column["dtype"]).lower()) or is_date_or_time_dtype((chunk_column["dtype"]).lower()) %}
            CAST(MAX({{ adapter.quote(chunk_column["column"]) }}) AS VARCHAR)
        {% else %}
            NULL
        {% endif %}   AS max
        , {% if is_numeric_dtype((chunk_column["dtype"]).lower()) %}
            ROUND(AVG({{ adapter.quote(chunk_column["column"]) }}), 2)
        {% else %}
            CAST(NULL AS NUMERIC)
        {% endif %}   AS avg
        , CAST('{{ current_date_and_time }}' AS TIMESTAMP_NTZ)    AS profiled_at
    FROM {{ source_table_name }}
{%- endmacro -%}



