with covic_cases as (

    SELECT date, country_name, subregion1_name, sum(coalesce(new_confirmed, 0)) as new_confirmed, sum(coalesce(new_deceased,0)) as new_deceased, sum(coalesce(new_recovered,0)) as new_recovered, sum(coalesce(new_tested,0)) as new_tested
    FROM `dop_sandbox_us.stg_covid19`
    {% if is_incremental() %}
        where date >= DATE("{{ ds }}")
    {% endif %}
    GROUP BY date, country_name, subregion1_name

)
select * from covic_cases