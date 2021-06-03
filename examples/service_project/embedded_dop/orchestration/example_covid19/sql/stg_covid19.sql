with covid_cases as (

    SELECT * REPLACE(CAST(new_recovered AS INT64) as new_recovered) FROM `bigquery-public-data.covid19_open_data.covid19_open_data`
    {% if is_incremental() %}
        where date >= DATE("{{ ds }}")
    {% endif %}

)
select * from covid_cases
