FROM google/cloud-sdk:slim

ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8

ARG DBT_HOME=/home/dbtuser
ARG BUILD_DIR=/tmp/dbt_build_tmp

RUN apt-get update && apt-get install -y git

RUN set -ex \
    && pip3 install PyYAML \
    && pip3 install pipenv

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
RUN DBT_HOME=${DBT_HOME} BUILD_DIR=${BUILD_DIR} python3 ${BUILD_DIR}/embedded_dop/source/infrastructure/executor/dbt/init.py

# remote the build dir
RUN rm -rf ${BUILD_DIR}
