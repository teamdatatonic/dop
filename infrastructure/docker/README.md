# Requirements files
## requirements.composer.txt

This matches to a specific Cloud Composer version documented here https://cloud.google.com/composer/docs/concepts/versioning/composer-versions
It is also used inside docker with the purpose to align pip packages with Composer as much as possible 

## requirements.txt
This contains extra pip dependencies required by the local Airflow environment in Docker. It depends on `requirements.composer.txt`