{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'date', 'data_type': 'date'}
  )
}}

with covic_cases as (

    select * REPLACE(CAST(new_recovered AS INT64) as new_recovered) from `{{ source('jaffle_shop', 'covid19_open_data') }}`
    {% if is_incremental() %}
        where date >= "{{ var('ds') }}" -- Same as DML merge for a range but the first run is always historical load and second run is incremental / don't need historical DAG anymore
    {% endif %}

)
select * from covic_cases