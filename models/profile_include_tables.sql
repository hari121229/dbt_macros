-- Pass one or many include_tables in the parameter.It will profile only the tables mentioned in the parameter.

{{
    config(
        materialized='ephemeral'
    )
}}


{{ data_quality.data_profiling('transforming_data',['transforming_test','transforming_demo'],[],['test_data'],'seed_data','seed','include_tables')}}