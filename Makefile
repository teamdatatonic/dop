.PHONY: build down up

DOP_PROJECT_ID := #{REPLACE WITH A GCP PROJECT ID WHERE DOP WILL EXECUTE ALL JOBS}
DOP_LOCATION := #{REPLACE WITH A GCP REGION WHERE DATA WILL BE PERSISTED BY DOP}

ENVS := PROJECT_ID=$(DOP_PROJECT_ID) \
	LOCATION=$(DOP_LOCATION)

validate:
	if [ -z ${DOP_PROJECT_ID} ]; then \
	echo "DOP_PROJECT_ID must be defined. Aborting";\
	exit 1; \
	elif [ -z ${DOP_LOCATION} ]; then \
	echo "DOP_LOCATION must be defined. Aborting";\
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