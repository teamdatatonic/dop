SELECT distinct country_name, subregion1_name
FROM `bigquery-public-data.covid19_open_data.covid19_open_data`
WHERE date >= execution_date