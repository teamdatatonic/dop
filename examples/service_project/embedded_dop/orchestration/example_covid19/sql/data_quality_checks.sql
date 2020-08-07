SELECT COUNT(*) > 0                                                    AS success,
       COUNT(*)                                                        AS num_of_records,
       'Do we have data in covid19_by_country for date >= "{{ ds }}"?' AS description
FROM `dop_sandbox_us.covid19_by_country`
{% if is_incremental() %}
WHERE date >= "{{ ds }}"
{% endif %}

UNION ALL

SELECT COUNT(*) > 0                                                              AS success,
       COUNT(*)                                                                  AS num_of_records,
       'Do we have data in covid19_by_country_and_region for date >= "{{ ds }}"' AS description
FROM `dop_sandbox_us.covid19_by_country_and_region`
{% if is_incremental() %}
WHERE date >= "{{ ds }}"
{% endif %}