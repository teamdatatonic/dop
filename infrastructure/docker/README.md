# Composer versions

Not every deployment uses the same version. In order to be able to test and run
those environments there are several `composer_{AIRFLOW_VERSION}` folders.

Each of them contains the necessary elements to build build its docker image and
to be run without an entry point or a command. This way they are interchangeable
when they get loaded by the `docker-compose-dop.yaml`, used by the Data 
Engineers, or when loaded by the `docker-compose.yaml` used by the core DOP
developers.

To use certain version it must be defined in one of the ways below:

1. Declared as a make variable inline in the CLI:
```
make build AIRFLOW_VERSION=1.10.15
```
2. Exported as an environment variable
```
export AIRFLOW_VERSION=1.10.15
make build
```
3. Defined in a `.env` file in the same folder as the Makefile to persist its
    value between terminal sessions and make `make` easier to call
```
echo 'AIRFLOW_VERSION=1.10.15' >> .env
make build
```

It could also be declared in the Makefile itself. But it's better to split
configuration and functionality.

# Requirements files
## composer-constrains.txt

This matches to a specific Cloud Composer version documented here https://cloud.google.com/composer/docs/concepts/versioning/composer-versions
It is also used inside docker with the purpose to align pip packages with Composer as much as possible

## requirements.txt
This contains extra pip dependencies required by the local Airflow environment in Docker. It depends on `requirements.composer.txt`
