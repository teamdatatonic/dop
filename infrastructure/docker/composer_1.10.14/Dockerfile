FROM apache/airflow:1.10.14-python3.6

# Install composer dependencies & additional required dependencies not included in Composer
COPY constrains-composer.txt requirements.txt ./
RUN set -ex && pip install --user -r requirements.txt

ENTRYPOINT airflow initdb; airflow scheduler & airflow webserver
