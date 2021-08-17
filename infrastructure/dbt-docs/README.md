# DBT docs

DBT documentation is generated using `dbt docs generate`. This command generates in the `target` folder of the project HTML documentation that can be served with any web server.

DBT also provides a command to serve documentation `dbt docs serve`. It starts a web server in the port 8080 (the port number can be changes using the `--port` param).

More information about these commands can be found at

    https://docs.getdbt.com/reference/commands/cmd-docs


## Generating DBT docs

DOP provides the option to generate DBT docs using a task in the `config.yaml` file.
This task generates the files and copy the files a bucket provided in the task arguments

      - identifier: dbt_start_docs
        kind:
          action: dbt
          target: docs generate
        options:
          project: PROJECT_NAME
          version: DBT_VERSION
          arguments:
            - option: --bucket
              value: DBT_DOCS_BUCKET
            - option: --bucket-path
              value: DBT_DOCS_PATH

The option `--bucket-path` is optional. If not present, files will be copied to the root folder of the bucket


## Serving a static website in Google Cloud

Once the files have been generated, Google Cloud provides several option to serve them as a static website:

    https://cloud.google.com/architecture/web-serving-overview


### GCS static site

The simplest option is to mark the bucket as public, and files can be accessed using the URL

    https://storage.googleapis.com/DBT_DOCS_BUCKET/DBT_DOCS_PATH/index.html

This is the simplest configuration option, but the website will be public to anyone that knows the URL.

To use an HTTPS custom domain, a load balancer is required

    https://cloud.google.com/storage/docs/hosting-static-website

GCS doesn't provide the option to have a private website requiring authentication.
There is a [feature request](https://issuetracker.google.com/issues/114133245?pli=1) to implement this functionality, but it's not implemented yet

To allow access to only authenticated users, an App Engine application is provided, described in the next section


## AppEngine

AppEngine allows authentication, but it doesn't allow writing in the filesystem.

DBT task described about creates files in GCS, but these files cannot be copied to the local disk to be served as static resources

The chosen approach has been to create a Flask application that reads docs files from GCS and serve them directly.
This application is in the `app-engine` folder

To avoid reading the files every request, a [cache](https://docs.python.org/3/library/functools.html#functools.lru_cache)
mechanism has been implemented that is cleared periodically.

Cache duration and bucket can be configured as environment parameters.
If not set in `app.yaml`, the following default values are used:

- BUCKET_NAME: Default AppEngine bucket (PROJECT_NAME.appspot.com)
- BUCKET_PATH: Empty. Files will be stored in the root folder
- CACHE_MAX_AGE_IN_SECONDS: 300 seconds. If previous request was received more than 5 minutes ago, cache is cleared


This app can be easily deployed running `gcloud app deploy` from the `app-engine` folder.
More information about deploying AppEngine can be found at

    https://cloud.google.com/appengine/docs/standard/python3/testing-and-deploying-your-app


By default, App Engine has public access, but it can be easily configured to use authentication using Identity-Aware Proxy

To configure it follow the instructions at

    https://cloud.google.com/iap/docs/app-engine-quickstart

To give access to all the users of the organisation, add as member `allAuthenticatedUsers` with role `IAP-secured Web App User`, and to restrict the access, apply the role just to the groups or users that should have access.
