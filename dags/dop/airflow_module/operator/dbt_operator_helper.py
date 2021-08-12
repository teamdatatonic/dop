import io
import json
import logging
import pathlib

from google.cloud import bigquery
from google.cloud import storage
from urllib.parse import urlparse
from google.cloud.exceptions import NotFound

DBT_RUN_RESULTS_TABLE = "run_results"
DBT_RUN_RESULTS_SCHEMA_FILE = "run_results_schema.json"


def implode_arguments(dbt_arguments, filter_func=None):
    filtered_dbt_arguments = (
        filter_func(dbt_arguments) if filter_func else dbt_arguments
    )
    return " ".join(
        [
            " ".join(
                [
                    argument["option"],
                    "" if argument.get("value") is None else argument.get("value"),
                ]
            )
            for argument in filtered_dbt_arguments
        ]
    )


def parsed_cmd_airflow_context_vars(context):
    cmd = '"{'
    context_vars = ["ds", "ds_nodash", "ts", "ts_nodash", "ts_nodash_with_tz"]
    if context:
        var_list = [f"'{v}'" + f": '{context[v]}'" for v in context_vars]
    else:
        var_list = [f"'{v}'" + ": '{{ " + v + " }}'" for v in context_vars]
    cmd += ",".join(var_list)

    cmd += '}"'

    return cmd


def save_run_results_in_bq(project_id, dbt_project_name, run_results_path):
    """
    Load run_results json file in BigQuery. As a first step is checked if the table
    already exists in the schema and if not, it's created.

    To fit BQ schema, the field metadata.env (JSON object) must be serialised
    and results.message converted to string,
    because depending on the task it can be an integer or a string
    """
    table_id = f"{project_id}.{dbt_project_name}.{DBT_RUN_RESULTS_TABLE}"
    client = bigquery.Client(project=project_id)
    check_run_results_table(client, table_id)

    run_results = {}
    if run_results_path.startswith("gs://"):
        storage_client = storage.Client()
        bucket, path = _parse_gcs_url(run_results_path)
        bucket = storage_client.get_bucket(bucket)
        blob = bucket.blob(path)
        run_results = json.loads(blob.download_as_string())
    else:
        with open(run_results_path) as run_results_file:
            run_results = json.load(run_results_file)

    if run_results["metadata"]["env"]:
        run_results["metadata"]["env"] = json.dumps(run_results["metadata"]["env"])
    else:
        del run_results["metadata"]["env"]
    for item in run_results["results"]:
        item["message"] = str(item["message"])

    data_as_file = io.StringIO(json.dumps(run_results))
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    )
    job = client.load_table_from_file(data_as_file, table_id, job_config=job_config)
    try:
        result = job.result()  # Waits for table load to complete.
        logging.info("Pushed {} rows into run_results table".format(result.output_rows))
    except Exception:
        logging.info(f"Error loading run_results to BigQuery: {job.errors}")


def check_run_results_table(client, table_id):
    """
    Check if run_results table exists in BigQuery, and if not create it
    """
    try:
        client.get_table(table_id)
    except NotFound:
        print("Table {} is not found.".format(table_id))
        current_folder = pathlib.Path(__file__).parent.absolute()
        schema = client.schema_from_json(
            f"{current_folder}/{DBT_RUN_RESULTS_SCHEMA_FILE}"
        )
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)


def _parse_gcs_url(gsurl):
    """
    Given a Google Cloud Storage URL (gs://<bucket>/<blob>), returns a
    tuple containing the corresponding bucket and blob.
    """
    parsed_url = urlparse(gsurl)
    bucket = parsed_url.netloc
    blob = parsed_url.path.lstrip("/")
    return bucket, blob
