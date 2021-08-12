import inspect
import logging
import os
import sys
from collections import defaultdict
from pydoc import locate
from typing import Dict, Any, List

import pendulum
from airflow.models import Variable, DagRun
from airflow.models.baseoperator import BaseOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.state import State

# Add DOP DAG root path to PYTHONPATH
sys.path.append(
    os.path.sep.join(
        [os.environ["DOP_SERVICE_PROJECT_PATH"], "embedded_dop", "source", "dags"]
    )
)

from dop import definitions  # noqa: E402
from dop.airflow_module.operator import (  # noqa: E402
    common as common_operators,
    dbt_operator,
    dbt_k8_operator,
)
from dop.airflow_module.dag_builder import dag_builder_util  # noqa: E402
from dop.component.transformation.common.parser.yaml_parser import (  # noqa: E402
    yaml_to_dict,
)
from dop.component.transformation.runner.bigquery.adapter import impl  # noqa: E402
from dop.component.transformation.runner.bigquery.adapter.impl import (  # noqa: E402
    QueryRunner,
)
from dop.component.transformation.runner.bigquery.adapter.relation import (  # noqa: E402
    BigQueryRelation as Relation,
)
from dop.component.transformation.common.adapter import schema  # noqa: E402
from dop.component.configuration.env import env_config  # noqa: E402

SQL_PATH_TEMPLATE = "{path}/sql/{task_name}.sql"


def locate_operator_class(namespaced_class: str):
    operator_class = locate(namespaced_class)

    if operator_class is None:
        raise RuntimeError(f"{namespaced_class} not found")

    if not inspect.isclass(operator_class):
        raise RuntimeError(f"{namespaced_class} is not a valid class")

    if not issubclass(operator_class, BaseOperator):
        raise RuntimeError(f"{namespaced_class} is not a valid operator")

    return operator_class


def select_operator(task: schema.Task):
    task_type_operator_mapper = {
        schema.TASK_KIND_MATERI: common_operators.MaterializationOperator,
        schema.TASK_KIND_INVOKE: common_operators.InvocationOperator,
        schema.TASK_KIND_ASSERT: common_operators.AssertOperator,
    }

    # DBT operator modifier (prd uses k8 pod operator)
    if env_config.is_sandbox_environment:
        task_type_operator_mapper[schema.TASK_KIND_DBT] = dbt_operator.DbtOperator
    else:
        task_type_operator_mapper[schema.TASK_KIND_DBT] = dbt_k8_operator.DbtK8Operator

    if task.kind.action == schema.TASK_KIND_AIRFLOW_OPERATOR:
        task_type_operator_mapper[
            schema.TASK_KIND_AIRFLOW_OPERATOR
        ] = locate_operator_class(task.kind.target)

    return task_type_operator_mapper[task.kind.action]


def load_sql_from_file(path, task_name):
    base_template = "{% from 'global.sql' import is_incremental with context %}"
    with open(SQL_PATH_TEMPLATE.format(path=path, task_name=task_name)) as fp:
        return base_template + "\n\n" + fp.read()


def create_relation_from_task(task):
    relation = Relation(
        database=task.database, schema=task.schema, identifier=task.identifier
    )

    return relation


def exec_replace_or_upsert(task: schema.Task, runner: QueryRunner, **kwargs):
    airflow_context = kwargs["airflow_context"]
    relation = create_relation_from_task(task=task)
    sql = airflow_context["templates_dict"]["sql"]

    dag_run_conf = (
        airflow_context["dag_run"].conf if airflow_context["dag_run"].conf else {}
    )

    full_refresh = dag_run_conf.get("full_refresh", False)

    if type(full_refresh) != bool:
        raise RuntimeError(
            "Dag config `full_refresh` must be set to either `true` or `false` (as a boolean value)."
        )

    logging.info(f"### IS FULL REFRESH ENABLED: {full_refresh}")

    runner.replace_or_upsert(
        query=sql,
        relation=relation,
        options={
            "partition_key": task.partitioning.field if task.partitioning else None,
            "partition_data_type": task.partitioning.data_type
            if task.partitioning
            else None,
            "full_refresh": full_refresh,
        },
    )


def exec_recreate_stored_procedure(task, runner: QueryRunner, **kwargs):
    airflow_context = kwargs["airflow_context"]
    relation = create_relation_from_task(task=task)
    sql = airflow_context["templates_dict"]["sql"]

    arguments = task.options.get("arguments", [])
    runner.recreate_stored_procedure(arguments=arguments, query=sql, relation=relation)


def exec_recreate_udf(task, runner: QueryRunner, **kwargs):
    airflow_context = kwargs["airflow_context"]
    relation = create_relation_from_task(task=task)
    sql = airflow_context["templates_dict"]["sql"]

    arguments = task.options.get("arguments", [])
    runner.recreate_udf(arguments=arguments, query=sql, relation=relation)


def exec_recreate_view(task, runner: QueryRunner, **kwargs):
    airflow_context = kwargs["airflow_context"]
    relation = create_relation_from_task(task=task)
    sql = airflow_context["templates_dict"]["sql"]

    runner.recreate_view(query=sql, relation=relation)


def exec_call_stored_procedure(task, runner: QueryRunner, **kwargs):
    airflow_context = kwargs["airflow_context"]
    sql = airflow_context["templates_dict"]["sql"]

    runner.call_stored_procedure(query=sql)


def exec_assertion(task, runner: QueryRunner, **kwargs):
    airflow_context = kwargs["airflow_context"]
    sql = airflow_context["templates_dict"]["sql"]

    runner.assertion(query=sql)


def exec_create_schema(task, runner: QueryRunner, **kwargs):
    runner.create_schema(project_id=task.database, dataset_id=task.schema)


def runner_caller(runner: QueryRunner, task: schema.Task, **kwargs):
    func = None

    if task.kind.action == schema.TASK_KIND_MATERI:
        if task.kind.target == "table":
            func = exec_replace_or_upsert
        elif task.kind.target == "stored_procedure":
            func = exec_recreate_stored_procedure
        elif task.kind.target == "view":
            func = exec_recreate_view
        elif task.kind.target == "udf":
            func = exec_recreate_udf
        elif task.kind.target == "schema":
            func = exec_create_schema
        else:
            raise NotImplementedError(
                f"Task Kind: {task.kind.__dict__} is not supported"
            )

    elif task.kind.action == schema.TASK_KIND_INVOKE:
        if task.kind.target == "stored_procedure":
            func = exec_call_stored_procedure
        else:
            raise NotImplementedError(
                f"Task Kind: {task.kind.__dict__} is not supported"
            )

    elif task.kind.action == schema.TASK_KIND_ASSERT:
        if task.kind.target == "assertion":
            func = exec_assertion
        else:
            raise NotImplementedError(
                f"Task Kind: {task.kind.__dict__} is not supported"
            )
    else:
        raise NotImplementedError(f"Task Kind: {task.kind.__dict__} is not supported")

    func(task=task, runner=runner, **kwargs)


def query_runner_callback(task, **kwargs):
    runner = impl.get_query_runner(
        options={
            "project_id": task.database,
            "location": env_config.location,
            "dry_run": False,
        }
    )

    runner_caller(runner=runner, task=task, airflow_context=kwargs)


def retrieve_dynamic_params(dag_id, dynamic_params):
    dynamic_params_with_values = {}
    for key, value in dynamic_params.items():
        full_var_key = f"{dag_id}.{key}"
        var_value = Variable.get(key=full_var_key, default_var=value)
        dynamic_params_with_values[key] = var_value

    return dynamic_params_with_values


def init_transformations(path_to_dags, config_extension="yaml") -> List[Dict[str, Any]]:
    details = []
    logging.debug("### Path to DAGs: {}".format(path_to_dags))
    directories = [d for d in os.listdir(path_to_dags) if "." not in d]
    logging.debug("### Scanning directories: {}".format(directories))

    for t in directories:
        t_path = os.path.join(path_to_dags, t)
        config_file = os.path.join(t_path, "config." + config_extension)
        logging.debug(f"### Unverified config file: {config_file}")
        if not os.path.exists(config_file):
            logging.debug("### Ignoring {}".format(config_file))
            continue
        with open(config_file) as config_fp:
            details.append(
                {
                    "transformation": t,
                    "path_to_transformation": t_path,
                    "config": yaml_to_dict(config_fp.read()),
                }
            )

    return details


def build(**kwargs):
    dags = []
    exceptions = []
    dbt_dags = defaultdict(set)

    for details in init_transformations(path_to_dags=kwargs["path_to_dags"]):
        logging.debug(f"### Dag Details: {details}")
        transformation = details["transformation"]
        path_to_transformation = details["path_to_transformation"]
        config = details["config"]

        dag_id = "dop__{}".format(transformation)
        try:
            # default database should not be set manually in config
            # but instead passed in based on implementation of each storage engine
            config["database"] = impl.get_database()
            dag_config = schema.load_dag_schema(payload=config)
        except schema.InvalidDagConfig as e:
            exceptions.append({"dag_id": dag_id, "e": e})
            continue

        # DAG will be excluded if disabled
        if not dag_config.enabled:
            logging.info(f"DAG {path_to_transformation} is disabled")
            continue

        common_template_path = os.path.join(
            definitions.ROOT_DIR,
            "component",
            "transformation",
            "common",
            "templating",
            "template",
        )

        dag = dag_builder_util.create_dag(
            dag_id=dag_id,
            start_date=dag_builder_util.get_default_dag_start_date(
                tzinfo=pendulum.timezone(dag_config.timezone)
            ),
            schedule_interval=dag_config.schedule_interval,
            template_searchpath=[path_to_transformation, common_template_path],
        )

        globals()[dag_id] = dag
        dags.append({"dag_id": dag_id, "dag": dag})

        transformation_tasks = {}

        template_params = {
            "params": dag_config.params,
        }

        # Indicates if the current DAG includes any DBT task
        has_dbt_tasks = False
        # If the DAG includes DBT tasks, projects where the tasks will be run
        # Used to ensure that only one DBT task is running per project
        dbt_projects = set()

        for task in dag_config.tasks:
            logging.debug(f"### task: {task.__dict__}")
            template_params["task"] = task.__dict__
            operator = select_operator(task=task)

            if task.kind.action in schema.NATIVE_TASK_KIND:
                if (
                    task.kind.action == schema.TASK_KIND_MATERI
                    and task.kind.target not in "schema"
                ) or task.kind.action != schema.TASK_KIND_MATERI:
                    sql = load_sql_from_file(
                        path=path_to_transformation, task_name=task.identifier
                    )
                else:
                    sql = None
                transformation_task = operator(
                    dag=dag,
                    task_id=task.identifier,
                    op_kwargs={"task": task},
                    templates_dict={"sql": sql},
                    params=template_params,
                    python_callable=query_runner_callback,
                    provide_context=True,
                )
                transformation_tasks[task.identifier] = transformation_task
            elif task.kind.action == schema.TASK_KIND_DBT:
                dbt_project_name = task.options["project"]
                transformation_task = operator(
                    dag=dag,
                    dbt_project_name=dbt_project_name,
                    dbt_version=task.options["version"],
                    dbt_arguments=task.options.get("arguments"),
                    task_id=task.identifier,
                    task=task,
                    params=template_params,
                    provide_context=True,
                )
                transformation_tasks[task.identifier] = transformation_task
                has_dbt_tasks = True
                dbt_dags[dbt_project_name].add(dag_id)
                dbt_projects.add(dbt_project_name)
            elif task.kind.action == schema.TASK_KIND_AIRFLOW_OPERATOR:
                if task.options.get("arguments"):
                    _kwargs = task.options.get("arguments")
                else:
                    _kwargs = {}

                transformation_task = operator(
                    dag=dag,
                    task_id=task.identifier,
                    task=task,
                    params=template_params,
                    provide_context=True,
                    **_kwargs,
                )
                transformation_tasks[task.identifier] = transformation_task
            else:
                raise NotImplementedError(f"Task Kind {task.kind} is not implemented")

        for task in dag_config.tasks:
            if len(task.dependencies) == 0:
                continue
            for dependency in task.dependencies:
                if dependency not in transformation_tasks:
                    raise RuntimeError("{}.sql does not exist".format(dependency))

                (
                    transformation_tasks[dependency]
                    >> transformation_tasks[task.identifier]
                )

        if has_dbt_tasks:
            # Add extra tasks to the DAG to ensure that only one DBT task
            # is running per project
            check_running_dags = BranchPythonOperator(
                dag=dag,
                task_id="check_running_dags",
                python_callable=check_running_dags_branching,
                op_kwargs={
                    "dbt_dags": dbt_dags,
                    "projects": dbt_projects,
                    "dag_id": dag_id,
                },
            )

            start_execution = DummyOperator(dag=dag, task_id="start_execution")
            skip_execution = DummyOperator(dag=dag, task_id="skip_execution")
            check_running_dags >> [skip_execution, start_execution]

            # Add start_execution upstream dependency to all tasks without upstream tasks
            for task in transformation_tasks.values():
                if not task.upstream_task_ids:
                    start_execution >> task

    return dags, exceptions


def check_running_dags_branching(dbt_dags: dict, projects: set, dag_id: str):
    """
    Check if there is any other DAG currently running in the same project(s) of the given DAG

    dbt_dags is a dict where the keys are DBT projects and the values the DAGs using the project
    {
        "project1: ["dag1", "dag2"],
        "project2: ["dag3"]
    }

    DAGS using the same project are extracted from dbt_dags and for each of them is checked
    if there is any running instance. If true execution is stopped

    :param dbt_dags: Dict of projects and DAGs
    :param projects: Set with the project used by the current DAG
    :param dag_id: Name of the current DAG
    """
    dags_to_check = set()
    for project in projects:
        project_dags = dbt_dags[project]
        project_dags.discard(dag_id)
        if project_dags:
            dags_to_check.update(project_dags)
    logging.info(f"Dags with DBT steps in the same projects: {dags_to_check}")
    for dag in dags_to_check:
        logging.info(f"Checking DAG {dag}")
        dag_runs = DagRun.find(dag_id=dag, state=State.RUNNING)
        if dag_runs:
            logging.info(f"DAG {dag} is running, skipping execution")
            return "skip_execution"
    logging.info("No other DAGs running in the same projects")
    return "start_execution"


dags, exceptions = build(path_to_dags=env_config.orchestration_path)

if exceptions:
    raise RuntimeError(
        "....".join(
            [
                f'#### Error for DAG {exception["dag_id"]}: {exception["e"]} ####'
                for exception in exceptions
            ]
        )
    )
