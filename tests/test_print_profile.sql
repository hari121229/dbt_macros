{% if execute %}

  {% do  data_profiling(['transforming_test','transforming_demo'],[],[],'seed_data','seed','test') %}

  
  -- Test passes if no exceptions are raised from the macro call (the actual output is not tested)
  {% set is_pass = True %}
  {% if not is_pass %}
    select 'fail'
  {% else %}
    select 'ok' limit 0
  {% endif %}
  
{% endif %}