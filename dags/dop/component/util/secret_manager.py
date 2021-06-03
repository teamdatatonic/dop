import logging

from google.cloud import secretmanager


def secret_client(credentials):
    return secretmanager.SecretManagerServiceClient(credentials=credentials)


def access_secret(project_id, secret_id, credentials, version="latest"):
    # Build the resource name of the secret version.
    client = secret_client(credentials=credentials)
    name = client.secret_version_path(project_id, secret_id, version)
    # Access the secret version.
    response = client.access_secret_version(name)

    payload = response.payload.data.decode("UTF-8")
    logging.info("Accessing secret version of {}".format(response.name))

    return payload
