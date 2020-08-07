{% set source_data = 'bigquery-public-data.covid19_open_data.covid19_open_data' %}

with covic_cases as (

    SELECT * FROM `{{ source_data }}`
    {% if is_incremental() %}
        where date >= DATE("{{ ds }}")
    {% endif %}

)
SELECT COUNT(*) > 0                                                    AS success,
       COUNT(*)                                                        AS num_of_records,
       'Do we have data available in `{{ source_data }}`?'             AS description
FROM covic_cases