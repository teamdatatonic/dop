FROM python:3.6-slim-buster

ARG DBT_HOME=/home/dbtuser
ARG BUILD_DIR=/tmp/dbt_build_tmp

RUN apt-get update && apt-get install -y git

# https://cloud.google.com/sdk/docs/install#deb
RUN apt-get update && \
    apt-get install -y curl gnupg && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && \
    apt-get update -y && \
    apt-get install google-cloud-sdk -y

RUN set -ex \
    && pip install PyYAML==5.3.1 \
    && pip install pipenv

RUN groupadd -g 999 dbtuser && useradd -r -u 999 -g dbtuser dbtuser
WORKDIR ${DBT_HOME}

RUN chown -R dbtuser:dbtuser ${DBT_HOME}

USER dbtuser
RUN mkdir ${DBT_HOME}/.dbt

RUN mkdir ${BUILD_DIR}

# Update pip dependencies
COPY --chown=dbtuser:dbtuser ./embedded_dop/executor_config/dbt/Pipfile ./embedded_dop/executor_config/dbt/Pipfile.lock ./
RUN pipenv sync

# store the whole service project repository in the .tmp folder and build what's required and then delete everything else
COPY --chown=dbtuser:dbtuser ./ ${BUILD_DIR}
RUN ls -al ${BUILD_DIR}

# initialise dbt
RUN DBT_HOME=${DBT_HOME} BUILD_DIR=${BUILD_DIR} python ${BUILD_DIR}/embedded_dop/source/infrastructure/executor/dbt/init.py

# remote the build dir
RUN rm -rf ${BUILD_DIR}
