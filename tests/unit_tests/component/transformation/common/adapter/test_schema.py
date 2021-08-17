import pytest
from dags.dop.component.transformation.common.adapter import (
    schema as transformation_schema,
)
from dags.dop.component.transformation.common.adapter.schema import InvalidDagConfig


def test_dag_config_overall_validation():
    invalid_payload = {}
    schema = transformation_schema.DagConfigSchema()
    errors = schema.load(invalid_payload).errors

    assert errors.get("timezone")
    assert errors.get("tasks")


def test_dag_config_cron_validation():
    invalid_payload = {"schedule_interval": "0 *"}
    schema = transformation_schema.DagConfigSchema()
    errors = schema.load(invalid_payload).errors

    assert errors.get("schedule_interval")


def test_task_overall_config_validation():
    invalid_payload = {
        "schedule_interval": "0 1 * * *",
        "timezone": "Europe/London",
        "tasks": [{}],
    }
    schema = transformation_schema.DagConfigSchema()
    errors = schema.load(invalid_payload).errors

    assert errors["tasks"][0].get("identifier")
    assert errors["tasks"][0].get("kind")


def test_task_kind_config_validation():
    invalid_payload = {
        "schedule_interval": "0 1 * * *",
        "timezone": "Europe/London",
        "tasks": [
            {
                "identifier": "stg_covid19",
                "kind": {
                    "action": "materialization_invalid",
                    "target": "table_invalid",
                },
            }
        ],
    }
    schema = transformation_schema.DagConfigSchema()
    errors = schema.load(invalid_payload).errors

    assert errors["tasks"][0].get("kind").get("action") == ["Not a valid choice."]
    assert errors["tasks"][0].get("kind").get("target") is None


def test_partitioning_validation_with_invalid_field():
    payload = generate_valid_schema()
    payload["tasks"] = [
        {"partitioning": {"field": "this_is_wrong", "data_type": "date"}}
    ]

    with pytest.raises(InvalidDagConfig):
        transformation_schema.load_dag_schema(payload)


def test_partitioning_validation_with_invalid_data_type():
    payload = generate_valid_schema()
    payload["tasks"] = [
        {"partitioning": {"field": "my_field", "data_type": "an_invalid_type"}}
    ]

    with pytest.raises(InvalidDagConfig):
        transformation_schema.load_dag_schema(payload)


def test_partitioning_validation_for_timestamp_type():
    valid_payload = generate_valid_schema()
    valid_payload["tasks"] = [
        {
            "partitioning": {"field": "date", "data_type": "timestamp"},
            "identifier": "stg_covid19",
            "schema": "dop_sandbox_us",
            "kind": {"action": "materialization", "target": "table"},
            "dependencies": ["a", "b", "c"],
        }
    ]

    dag_config = transformation_schema.load_dag_schema(valid_payload)

    assert isinstance(
        dag_config.tasks[0].partitioning, transformation_schema.Partitioning
    )
    assert dag_config.tasks[0].partitioning.data_type == "timestamp"


def test_partitioning_validation_for_datetime_type():
    valid_payload = generate_valid_schema()
    valid_payload["tasks"] = [
        {
            "partitioning": {"field": "date", "data_type": "datetime"},
            "identifier": "stg_covid19",
            "schema": "dop_sandbox_us",
            "kind": {"action": "materialization", "target": "table"},
            "dependencies": ["a", "b", "c"],
        }
    ]

    dag_config = transformation_schema.load_dag_schema(valid_payload)

    assert isinstance(
        dag_config.tasks[0].partitioning, transformation_schema.Partitioning
    )
    assert dag_config.tasks[0].partitioning.data_type == "datetime"


def test_partitioning_validation_for_date_type():
    valid_payload = generate_valid_schema()
    valid_payload["tasks"] = [
        {
            "partitioning": {"field": "date", "data_type": "date"},
            "identifier": "stg_covid19",
            "schema": "dop_sandbox_us",
            "kind": {"action": "materialization", "target": "table"},
            "dependencies": ["a", "b", "c"],
        }
    ]

    dag_config = transformation_schema.load_dag_schema(valid_payload)

    assert isinstance(
        dag_config.tasks[0].partitioning, transformation_schema.Partitioning
    )
    assert dag_config.tasks[0].partitioning.data_type == "date"


def test_schema_deserialization():
    payload = generate_valid_schema()

    try:
        transformation_schema.load_dag_schema(payload)
    except transformation_schema.InvalidDagConfig as e:
        pytest.fail(f"Should not raise exception InvalidDagConfig, error: {e}")

    dag_config = transformation_schema.load_dag_schema(payload)
    assert isinstance(dag_config, transformation_schema.DagConfig)
    assert isinstance(dag_config.tasks[0], transformation_schema.Task)
    assert isinstance(dag_config.tasks[0].kind, transformation_schema.Kind)
    assert isinstance(
        dag_config.tasks[0].partitioning, transformation_schema.Partitioning
    )
    assert dag_config.tasks[0].dependencies == ["a", "b", "c"]


def generate_valid_schema():
    return {
        "schedule_interval": "0 1 * * *",
        "timezone": "Europe/London",
        "params": {"value_a": [1, 2, 3]},
        "database": "sandbox",
        "schema": "dop_sandbox_us",
        "tasks": [
            {
                "partitioning": {"field": "date", "data_type": "date"},
                "identifier": "stg_covid19",
                "schema": "dop_sandbox_us",
                "kind": {"action": "materialization", "target": "table"},
                "dependencies": ["a", "b", "c"],
            }
        ],
    }
