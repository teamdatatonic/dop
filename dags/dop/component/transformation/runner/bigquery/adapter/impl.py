import logging
import jinja2

from typing import Optional, Dict, Any, List
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError, NotFound

from dop.component.configuration import env
from dop.component.transformation.common.parser import yaml_parser
from dop.component.transformation.common.templating import jinja
from dop.component.transformation.runner.bigquery.adapter.model import (
    TableOptionsConfig,
    PartitionConfig,
    UDFArgument,
    StoredProcedureArgument,
)
from dop.component.transformation.runner.bigquery.adapter.relation import (
    BigQueryRelation as Relation,
    RelationHelper,
)
from dop.component.util import auth


def get_query_job_config(
    destination,
    write_disposition=bigquery.WriteDisposition.WRITE_EMPTY,
    create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
):
    job_config = bigquery.QueryJobConfig()
    job_config.destination = destination
    job_config.write_disposition = write_disposition
    job_config.create_disposition = create_disposition

    return job_config


def execute_job_with_error_logging(job):
    if job.dry_run:
        logging.info(
            f"Total GB it will process: {job.total_bytes_processed / 1024 / 1024 / 1024}"
        )
        return

    try:
        job.result()
        logging.info("Affected: {} rows".format(job.num_dml_affected_rows))
        logging.info("Job completed...")
    except GoogleCloudError as e:
        logging.error(e)
        logging.error(job.error_result)
        logging.error(job.errors)
        raise e


def get_database():
    return env.env_config.project_id


def get_bq_client(project_id, location, credentials):
    return bigquery.client.Client(
        project=project_id, location=location, credentials=credentials
    )


def get_query_runner(options: Optional[Dict[str, Any]]):
    dry_run = options.get("dry_run", True)
    project_id = options["project_id"]
    location = options["location"]
    credentials = None

    if not project_id:
        raise ValueError("BigQuery requires a project_id")

    if env.env_config.is_sandbox_environment:
        credentials = auth.ServiceAccountImpersonationCredentialManager(
            source_sa_name=env.DOP_DOCKER_USER, project_id=project_id
        ).get_target_credentials()
        logging.info(
            f"Using service account impersonation, and service account `{env.DOP_DOCKER_USER}` is now active"
        )

    client = get_bq_client(
        project_id=project_id, location=location, credentials=credentials
    )
    jinja_environment = jinja.get_runner_environment(runner="bigquery")
    return QueryRunner(
        client=client, jinja_environment=jinja_environment, dry_run=dry_run
    )


class QueryRunner:
    def __init__(
        self,
        client: bigquery.client.Client,
        jinja_environment: jinja2.Environment,
        dry_run=True,
    ):
        self._client = client
        self._jinja_environment = jinja_environment
        self._dry_run = dry_run
        self._relation_helper = RelationHelper(client=self._client)

    def write_append(self, query, relation):
        job_config = get_query_job_config(
            destination=relation,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        logging.info(f"Appending data to {relation} using query: {query}")

        query_job = self._client.query(query=query, job_config=job_config)
        execute_job_with_error_logging(job=query_job)

    def replace_or_upsert(
        self, query: str, relation: Relation, options: Optional[Dict[str, Any]] = None
    ):
        """
        TODO: this has quite a bit of duplication, needs tidying up
        This does a full replacement or an upsert to the target relation.
        The function has the following behaviour
        1. When the target table does not exist, it creates the table using schema inferred by the query results
        2. If the target table is already there, it writes into a temp table first and then merge to the target table so the process is atomic and will break if the schema is changed

        :param query: SQL Query
        :param relation: BigQuery relation to write truncate
        :param options: For options such as specifying partitions, forcing full refresh etc
        Range based Partition:
        options={'partition_key': 'key', 'partition_data_type': 'int64', 'partition_range': {'start': 0, 'end': 1000, 'interval': 100 } }

        Date based Partition:
        options={'partition_key': 'key', 'partition_data_type': 'datetime'}

        """

        def query_cleaned(q):
            if q.strip()[:-1] == ";":
                q = q.strip()[:-1]
                if q.strip()[:-1] == ";":
                    raise RuntimeError(
                        f"Query {query} should not be followed by `;` at the very end"
                    )

            return q

        query = query_cleaned(q=query)

        options = {} if not options else options
        full_refresh = options.get("full_refresh", False)

        tmp_relation = Relation(
            database=relation.database,
            schema=relation.schema,
            identifier="_tmp_" + relation.identifier,
        )

        template_create_or_replace = self._jinja_environment.from_string(
            """
            {% import 'materialization/table_create_or_replace.sql' as materialise_table %}
            {{ materialise_table.create_or_replace(query, relation_helper, options) }}
        """
        )

        table_options_config = TableOptionsConfig(
            options={
                "expiration_timestamp": "TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 hour)"
            }
        )

        partition_config = PartitionConfig.create(options=options)

        rendered_create_or_replace = template_create_or_replace.render(
            query=query,
            relation_helper=self._relation_helper,
            options={
                "relation": relation,
                "tmp_relation": tmp_relation,
                "table_options_config": table_options_config,
                "partition_config": partition_config,
                "full_refresh": full_refresh,
            },
        )

        logging.info("Running Query: {}".format(rendered_create_or_replace))

        job_config = bigquery.QueryJobConfig(dry_run=self._dry_run)
        query_job = self._client.query(
            query=rendered_create_or_replace, job_config=job_config
        )

        execute_job_with_error_logging(job=query_job)

        template_upsert = self._jinja_environment.from_string(
            """
                    {% import 'materialization/table_upsert.sql' as materialise_table %}
                    {{ materialise_table.upsert(query, relation_helper, options) }}
                """
        )

        rendered_upsert = template_upsert.render(
            query=query,
            relation_helper=self._relation_helper,
            options={
                "relation": relation,
                "tmp_relation": tmp_relation,
                "partition_config": partition_config,
                "full_refresh": full_refresh,
            },
        )

        logging.info("Running the Upsert Query: {}".format(rendered_upsert))

        job_config = bigquery.QueryJobConfig(dry_run=self._dry_run)
        query_job = self._client.query(query=rendered_upsert, job_config=job_config)

        execute_job_with_error_logging(job=query_job)

    def recreate_view(self, query, relation: Relation):
        full_table_id = f"{relation.database}.{relation.schema}.{relation.identifier}"
        view = bigquery.table.Table(table_ref=full_table_id)
        view.view_query = query

        logging.info("Executing query: {}".format(query))

        try:
            self._client.delete_table(view)
        except NotFound:
            logging.info(
                "View {} not found, ignore the delete operation".format(relation)
            )
            pass

        self._client.create_table(view)

        logging.info("View: {} has been created".format(relation))

    def create_schema(self, project_id, dataset_id, exists_ok=True):
        # TODO: add support to dataset level TTL
        full_dataset_id = f"{project_id}.{dataset_id}"
        dataset = bigquery.dataset.Dataset(dataset_ref=full_dataset_id)

        self._client.create_dataset(dataset=dataset, exists_ok=exists_ok)
        logging.info(
            "New Dataset {} already exists or has been created".format(full_dataset_id)
        )

    def recreate_udf(self, arguments: List[Dict], query, relation: Relation):
        if type(arguments) != list:
            raise TypeError(
                "arguments for UDF must be a list of entities with `name` and `type`. "
                "Please refer to the documentation for an example"
            )

        query = self.render_udf_query(
            arguments=arguments, query=query, relation=relation
        )
        logging.info("Creating the UDF using: {}".format(query))

        job_config = bigquery.QueryJobConfig(dry_run=self._dry_run)
        query_job = self._client.query(query=query, job_config=job_config)

        execute_job_with_error_logging(job=query_job)

        logging.info("UDF: {} has been created".format(relation))

    def recreate_stored_procedure(self, arguments: List[Dict], query, relation):
        if type(arguments) != list:
            raise TypeError(
                "arguments for Stored Procedure must be a list of entities with `name` and `type`. "
                "Please refer to the documentation for an example"
            )

        query = self.render_stored_procedure_query(
            arguments=arguments, query=query, relation=relation
        )
        logging.info(f"Creating the Stored Procedure using query: {query}")

        job_config = bigquery.QueryJobConfig()
        query_job = self._client.query(query=query, job_config=job_config)
        execute_job_with_error_logging(job=query_job)

        logging.info("Stored Procedure: {} has been created".format(relation))

    @staticmethod
    def render_udf_query(arguments: List[Dict], query, relation):
        arguments_models = [
            UDFArgument(name=argument["name"], type=argument["type"])
            for argument in arguments
        ]
        parsed_arguments = ",".join(
            [f"{argument.name} {argument.type}" for argument in arguments_models]
        )
        rendered_query = (
            f"""
CREATE OR REPLACE FUNCTION {relation}({parsed_arguments}) AS
(
   """
            + query
            + """
)
"""
        )

        return rendered_query

    @staticmethod
    def render_stored_procedure_query(arguments: List[Dict], query, relation):
        arguments_models = [
            StoredProcedureArgument(name=argument["name"], type=argument["type"])
            for argument in arguments
        ]
        parsed_arguments = ",".join(
            [f"{argument.name} {argument.type}" for argument in arguments_models]
        )
        rendered_query = f"""
CREATE OR REPLACE PROCEDURE {relation}({parsed_arguments})
BEGIN
  {query};
END;
"""

        return rendered_query

    def assertion(self, query):
        def compile_assertion_results(rows):
            assertion_results = []
            has_failure = False
            reserved_keys = ["success", "description"]
            for row in rows:
                assertion_result = {
                    "success": None,
                    "description": None,
                    "other_asserted_values": {},
                }
                if not row["success"]:
                    has_failure = True

                assertion_result["success"] = row["success"]
                assertion_result["description"] = row["description"]
                for key, value in row.items():
                    if key not in reserved_keys:
                        assertion_result["other_asserted_values"][key] = value

                assertion_results.append(assertion_result)

            return {"has_failure": has_failure, "assertion_results": assertion_results}

        job_config = bigquery.QueryJobConfig()
        query_job = self._client.query(query=query, job_config=job_config)

        logging.info("Running assertion using query: {}".format(query))

        try:
            results = compile_assertion_results(rows=query_job.result())

            logging.info(
                "\n\n#### Assertion Report ####\n\n"
                + yaml_parser.dict_to_yaml(results["assertion_results"])
                + "\n\n#### Assertion Report ####\n\n"
            )

            if results["has_failure"]:
                raise AssertionError(
                    'Assertion failed, check the "ASSERTION RESULTS" section for more details'
                )

        except GoogleCloudError as e:
            logging.error(e)
            logging.error(query_job.error_result)
            logging.error(query_job.errors)
            raise e

    def call_stored_procedure(self, query):
        job_config = bigquery.QueryJobConfig()

        sp = f"""
            BEGIN
                {query}
            END;
            """
        logging.info(f"Calling Stored Procedure(s) :{query}")

        query_job = self._client.query(query=sp, job_config=job_config)
        execute_job_with_error_logging(job=query_job)
