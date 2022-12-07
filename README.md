# dbt-profiler

`dbt-profiler` implements dbt macros for profiling database relations and creating  `doc` blocks and table schemas (`schema.yml`) containing said profiles. A calculated profile contains the following measures for each column in a relation:

* `database`: Name of the column
* `schema`: Name of the column
* `table_name`: Name of the column
* `row_count`: Column based row count
* `column_name`: Name of the column
* `data_type`: Data type of the column
* `not_null_proportion`: Proportion of column values that are not `NULL` (e.g., `0.62` means that 62% of the values are populated while 38% are `NULL`)
* `distinct_proportion`: Proportion of unique column values (e.g., `1` means that 100% of the values are unique)
* `distinct_count`: Count of unique column values
* `is_unique`: True if all column values are unique
* `min`: Minimum column value
* `max`: Maximum column value
* `avg`: Average column value
* `profiled_at`: Profile calculation date and time

`*` numeric, date and time columns only
`**` numeric columns only
`^` can be excluded from the profile using `exclude_measures` argument

## Purpose 

`dbt-profiler` aims to provide the following:

1. [data_profile](#get_profile-source) macro for generating profiling SQL queries that can be used as dbt models or ad-hoc queries
2. Describe a mechanism to include model profiles in [dbt docs](https://docs.getdbt.com/docs/building-a-dbt-project/documentation)

## Installation

`dbt-profiler` requires - git: https://github.com/hari121229/dbt_macros_hub.git
                          revision: v1.0.5 


# Macros

## data_profile ([source](macros/get_profile.sql))

This macro returns a relation profile as a SQL query that can be used in a dbt model. This is handy for previewing relation profiles in dbt Cloud.

### Arguments
* `source_database` (required): Mention the source table name.
* `source_schema` (required): Mention the source schema name
* `include_tables` (optional): List of columns to include in the profile (default: `[]` i.e., all). Only one of `include_tables` and `include_table` can be specified at a time.
* `exclude_tables` (optional): List of columns to exclude from the profile (default: `[]`). Only one of `include_tables` and `exclude_tables` can be specified at a time.
* `destination_database` (required): Mention the destination table name.
* `destination_schema` (required): Mention the destination table name.
* `destination_table` (required): Mention the destination table name.
### Usage

```sql
{{
    config(
        materialized='ephemeral'
    )
}}


{{ data_quality.data_profiling('source_database',['source_schema'],['include_tables],['exclude_tables'],'destination_database','destination_schema','destination_table')}}
```
