# dbt-profiler

`dbt-profiler` implements dbt macros for profiling database relations and creating  `doc` blocks and table schemas (`schema.yml`) containing said profiles. A calculated profile contains the following measures for each column in a relation:

* `database`: Name of the column
* `schema`: Name of the column
* `table_name`: Name of the column
* `row_count`: Column based row count
* `column_name`: Name of the column
* `data_type`: Data type of the column
* `not_null_count`: Count the not_null values by column based
* `null_count`: Count the null values by column based.
* `not_null_percentage`: Percentage of column values that are not `NULL` (e.g., `0.62` means that 62% of the values are populated while 38% are `NULL`)
* `null_percentage`: Percentage of column values that are not `NOT_NULL` (e.g., `0.55` means that 55% of the values are populated while 45% are `NOT_NULL`)
* `distinct_percentage`: Percentage of unique column values (e.g., `1` means that 100% of the values are unique)
* `distinct_count`: Count of unique column values
* `is_unique`: True if all column values are unique
* `min`: Minimum column value
* `max`: Maximum column value
* `avg`: Average column value
* `profiled_at`: Profile calculation date and time

## Purpose 

`dbt-profiler` aims to provide the following:

1. [data_profile](#get_profile-source) macro for generating profiling SQL queries that can be used as dbt models or ad-hoc queries
2. Describe a mechanism to include model profiles in [dbt docs](https://docs.getdbt.com/docs/building-a-dbt-project/documentation)

## Installation
 dbt version required: >=1.1.0.
 Include the following in your packages.yml file:
```sql
packages:
  - git: https://github.com/hari121229/dbt_macros_hub.git
    revision: v1.0.5 
```

## Supported adapters

✅ Snowflake

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
### Example Output

| column_name             | data_type | not_null_proportion | distinct_proportion | distinct_count | is_unique | min        | max        |                 avg |  std_dev_population |      std_dev_sample | profiled_at                   |
| ----------------------- | --------- | ------------------- | ------------------- | -------------- | --------- | ---------- | ---------- | ------------------- | ------------------- | ------------------- | ----------------------------- |
| customer_id             | int64     |                1.00 |                1.00 |            100 |         1 | 1          | 100        | 50.5000000000000000 | 28.8660700477221200 | 29.0114919758820200 | 2022-01-13 10:14:48.300040+00 |
| first_order             | date      |                0.62 |                0.46 |             46 |         0 | 2018-01-01 | 2018-04-07 |                     |                     |                     | 2022-01-13 10:14:48.300040+00 |
| most_recent_order       | date      |                0.62 |                0.52 |             52 |         0 | 2018-01-09 | 2018-04-09 |                     |                     |                     | 2022-01-13 10:14:48.300040+00 |
| number_of_orders        | int64     |                0.62 |                0.04 |              4 |         0 | 1          | 5          |  1.5967741935483863 |  0.7716692718648833 |  0.7779687173818426 | 2022-01-13 10:14:48.300040+00 |
| customer_lifetime_value | float64   |                0.62 |                0.35 |             35 |         0 | 1          | 99         | 26.9677419354838830 | 18.6599171435558730 | 18.8122455252636630 | 2022-01-13 10:14:48.300040+00 |
