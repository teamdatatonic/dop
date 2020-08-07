{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'date', 'data_type': 'date'}
  )
}}

with covic_cases as (

    SELECT date, country_name, subregion1_name, sum(coalesce(new_confirmed, 0)) as new_confirmed, sum(coalesce(new_deceased,0)) as new_deceased, sum(coalesce(new_recovered,0)) as new_recovered, sum(coalesce(new_tested,0)) as new_tested
    FROM {{ ref('stg_covid19_cases') }}
    {% if is_incremental() %}
        where date >= "{{ var('ds') }}"
    {% endif %}
    GROUP BY date, country_name, subregion1_name

)
select * from covic_cases