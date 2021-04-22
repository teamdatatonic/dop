FROM apache/airflow:1.10.15-python3.6

# Install composer dependencies & additional required dependencies not included in Composer
COPY constrains-composer.txt requirements.txt ./
RUN set -ex && pip install --user -r requirements.txt

ENTRYPOINT airflow db init; airflow scheduler & airflow webserver
