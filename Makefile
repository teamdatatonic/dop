.PHONY: build down up

# Defaults to the latest
AIRFLOW_VERSION := 1.10.15

include .env
export

ENVS := PROJECT_ID=$(DOP_PROJECT_ID) \
	LOCATION=$(DOP_LOCATION)

validate:
	if [ -z ${DOP_PROJECT_ID} ]; then \
	echo "DOP_PROJECT_ID must be defined. Aborting";\
	exit 1; \
	elif [ -z ${DOP_LOCATION} ]; then \
	echo "DOP_LOCATION must be defined. Aborting";\
	exit 1; \
	elif [ -z ${AIRFLOW_VERSION} ]; then \
	echo "AIRFLOW_VERSION must be defined. Aborting";\
	exit 1; \
	fi

build:
	$(ENVS) docker-compose up -d --build webserver

down: validate
	$(ENVS) docker-compose down

up:
	$(ENVS) docker-compose up -d

logs:
	docker logs dop_webserver -f