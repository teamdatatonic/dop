FROM apache/airflow:1.10.10-2-python3.6
LABEL maintainer="Datatonic"

ARG AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW_HOME=${AIRFLOW_HOME}

USER root
# Install dos2unix used to resolve windows line ending issues
# And gcc used in dbt packages compilation
RUN apt-get update && apt-get install dos2unix gcc -y

USER airflow

# Install composer dependencies & additional required dependencies not included in Composer
COPY composer_1.10.10/requirements.composer.txt /requirements.composer.txt
COPY requirements.txt /pre-installed-requirements.txt
RUN set -ex \
    && pip install --user -r /pre-installed-requirements.txt

COPY --chown=airflow:airflow script/entrypoint.sh ${AIRFLOW_HOME}/script/entrypoint.sh
COPY --chown=airflow:airflow script/exec_entrypoint.sh ${AIRFLOW_HOME}/script/exec_entrypoint.sh

# Resolve windows line ending issues
RUN dos2unix -n ${AIRFLOW_HOME}/script/entrypoint.sh ${AIRFLOW_HOME}/script/entrypoint.sh
RUN dos2unix -n ${AIRFLOW_HOME}/script/exec_entrypoint.sh ${AIRFLOW_HOME}/script/exec_entrypoint.sh

# allow execution of entrypoint script
RUN chmod +x ${AIRFLOW_HOME}/script/entrypoint.sh
