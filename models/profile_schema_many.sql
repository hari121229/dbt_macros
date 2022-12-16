-- Pass many schema_name in the parameter

{{
    config(
        materialized='ephemeral'
    )
}}


{{ data_quality.data_profiling('transforming_data',['transforming_test','transforming_demo'],[],[],'seed_data','seed','schema_name_many')}}