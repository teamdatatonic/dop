.PHONY: build down up up-follow logs exec restart

DOP_GIT_SSH_REPO_PATH := #{REPLACE WITH THE SSH GIT REPO PATH OF DOP}
DOP_PROJECT_ID := #{REPLACE WITH A GCP PROJECT ID WHERE DOP WILL EXECUTE ALL JOBS}
DOP_LOCATION := #{REPLACE WITH A GCP REGION WHERE DATA WILL BE PERSISTED BY DOP}
DOP_ARTIFACTS_BUCKET := #{REPLACE WITH ARTIFACT BUCKET NAME}
DOP_INFRA_PROJECT_ID := #{REPLACE WITH THE GCP INFRASTRUCTURE PROJECT ID WHERE BUILD ARTIFACTS ARE STORED}

REPO_BASE_NAME := $(shell basename `git rev-parse --show-toplevel`)
BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
HASH := $(shell git rev-parse HEAD | head -c7)
SERVICE_PROJECT_ABS_PATH := $(shell pwd)
DATETIME := $(shell date '+%Y%m%d-%H%M%S')

LOCAL_DOP_EMBEDDED_SOURCE_PATH := ./embedded_dop/source
DOP_TAG_NAME := master # Designed to be overwritten if a TAG or a different branch needs to be used for deployment

ENVS := PROJECT_ID=$(DOP_PROJECT_ID) \
	LOCATION=$(DOP_LOCATION) \
	SERVICE_PROJECT_ABS_PATH=$(SERVICE_PROJECT_ABS_PATH)

validate:
	if [ -z ${DOP_GIT_SSH_REPO_PATH} ]; then \
	echo "DOP_GIT_SSH_REPO_PATH must be defined. Aborting";\
	exit 1; \
	elif [ -z ${DOP_PROJECT_ID} ]; then \
	echo "DOP_PROJECT_ID must be defined. Aborting";\
	exit 1; \
	elif [ -z ${DOP_LOCATION} ]; then \
	echo "DOP_LOCATION must be defined. Aborting";\
	exit 1; \
	elif [ -z ${DOP_ARTIFACTS_BUCKET} ]; then \
	echo "DOP_ARTIFACTS_BUCKET must be defined. Aborting";\
	exit 1; \
	elif [ -z ${DOP_INFRA_PROJECT_ID} ]; then \
	echo "DOP_INFRA_PROJECT_ID must be defined. Aborting";\
	exit 1; \
	fi

validate-deploy:
	if [ -z ${DEPLOY_BUCKET_NAME} ]; then \
	echo "DEPLOY_BUCKET_NAME must be defined. Aborting";\
	exit 1; \
	elif [ -z ${DOP_ARTIFACT_ID} ]; then \
	echo "DOP_ARTIFACT_ID must be defined. Aborting";\
	exit 1; \
	fi

clean:
	docker rm -f $(docker ps -a | grep dop_ | awk '{print $1}')

git-checkout-dop: validate
	git clone $(DOP_GIT_SSH_REPO_PATH) $(LOCAL_DOP_EMBEDDED_SOURCE_PATH) 2> /dev/null || git -C $(LOCAL_DOP_EMBEDDED_SOURCE_PATH) clean -fdx && git -C $(LOCAL_DOP_EMBEDDED_SOURCE_PATH) remote update && (git -C $(LOCAL_DOP_EMBEDDED_SOURCE_PATH) reset --hard origin/$(DOP_TAG_NAME) || git -C $(LOCAL_DOP_EMBEDDED_SOURCE_PATH) checkout $(DOP_TAG_NAME))

build: git-checkout-dop
	$(ENVS) docker-compose -f $(LOCAL_DOP_EMBEDDED_SOURCE_PATH)/infrastructure/docker/docker-compose-dop.yml up -d --build webserver

down: validate
	$(ENVS) docker-compose -f $(LOCAL_DOP_EMBEDDED_SOURCE_PATH)/infrastructure/docker/docker-compose-dop.yml down

up: git-checkout-dop
	$(ENVS) docker-compose -f $(LOCAL_DOP_EMBEDDED_SOURCE_PATH)/infrastructure/docker/docker-compose-dop.yml up -d

up-follow: validate
	$(ENVS) docker-compose -f $(LOCAL_DOP_EMBEDDED_SOURCE_PATH)/infrastructure/docker/docker-compose-dop.yml up

restart:
	make down && make up

logs:
	docker logs dop_webserver -f

exec:
	docker exec -it dop_webserver /bin/bash -c "source ./script/exec_entrypoint.sh; /bin/bash"

executor-example-dbt-run: validate
	docker run --workdir "/home/dbtuser/dbt_start" --env DOP_PROJECT_ID=$(DOP_PROJECT_ID) --env DOP_LOCATION=$(DOP_LOCATION) dop-dbt:latest /bin/bash -c "pipenv run dbt run"

build-dbt-image:
	gcloud builds submit \
		--substitutions SHORT_SHA=$(HASH),_DATETIME=$(DATETIME) \
        --config=$(LOCAL_DOP_EMBEDDED_SOURCE_PATH)/infrastructure/cloudbuild/build-dbt.yaml \
        --project=$(DOP_INFRA_PROJECT_ID) \
        .

build-artifact: git-checkout-dop build-dbt-image
	gcloud builds submit \
	    --substitutions SHORT_SHA=$(HASH),BRANCH_NAME=$(BRANCH),REPO_NAME=$(REPO_BASE_NAME),_CLOUDBUILD_ARTIFACTS_BUCKET_NAME=$(DOP_ARTIFACTS_BUCKET),_DATETIME=$(DATETIME) \
        --config=$(LOCAL_DOP_EMBEDDED_SOURCE_PATH)/infrastructure/cloudbuild/build.yaml \
        --project=$(DOP_INFRA_PROJECT_ID) \
        .

deploy: validate validate-deploy
	gcloud builds submit \
	    --substitutions REPO_NAME=$(REPO_BASE_NAME),_DOP_ARTIFACT_ID=$(DOP_ARTIFACT_ID),_CLOUDBUILD_ARTIFACTS_BUCKET_NAME=$(DOP_ARTIFACTS_BUCKET),_DEPLOY_BUCKET_NAME=$(DEPLOY_BUCKET_NAME) \
        --config=$(LOCAL_DOP_EMBEDDED_SOURCE_PATH)/infrastructure/cloudbuild/deploy.yaml \
        --project=$(DOP_INFRA_PROJECT_ID) \
        .
