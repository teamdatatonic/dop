import os
import time
from functools import lru_cache

from flask import Flask
from google.cloud import storage

app = Flask(__name__)

# Bucket where dbt docs are stored
DBT_BUCKET_NAME = os.getenv("DBT_BUCKET_NAME")
if not DBT_BUCKET_NAME:
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
    if not project_id:
        raise ValueError(
            "'GOOGLE_CLOUD_PROJECT' or 'BUCKET_NAME' env variable must be set"
        )
    DBT_BUCKET_NAME = f"{project_id}.appspot.com"

# Path in the bucket where dbt docs are stored
DBT_BUCKET_PATH = os.getenv("DBT_BUCKET_PATH", "")

# Bucket files will be cached during this period
CACHE_MAX_AGE_IN_SECONDS = os.getenv("CACHE_MAX_AGE_IN_SECONDS", 300)

storage_client = storage.Client()
bucket = storage_client.bucket(DBT_BUCKET_NAME)
# Used for cache expiration
last_cache_reload = time.time()


@app.route("/")
def index():
    """
    Read index.html file from GCS bucket
    """
    return read_gcs_blob("index.html")


@app.route("/catalog.json")
def catalog():
    """
    Read catalog.json file from GCS bucket
    """
    return read_gcs_blob("catalog.json")


@app.route("/manifest.json")
def manifest():
    """
    Read manifest.json file from GCS bucket
    """
    return read_gcs_blob("manifest.json")


@lru_cache(maxsize=3)
def read_gcs_blob(name):
    """
    Read a blob from GCS

    :param name: blob to be read from GCS
    :return: blob content
    """
    path = f"{DBT_BUCKET_PATH}/{name}" if DBT_BUCKET_PATH else name
    blob = bucket.blob(path)
    return blob.download_as_bytes().decode("utf-8")


@app.before_request
def before_request():
    """
    function to run before each request. Clear the cache if expired
    """
    global last_cache_reload
    if time.time() - last_cache_reload > CACHE_MAX_AGE_IN_SECONDS:
        print("Clearing cache")
        read_gcs_blob.cache_clear()
        last_cache_reload = time.time()


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=True)
