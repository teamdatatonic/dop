import pytest
from dop.component.transformation.common.adapter import schema as transformation_schema


def test_dag_config_overall_validation():
    invalid_payload = {
    }
    schema = transformation_schema.DagConfigSchema()
    errors = schema.load(invalid_payload).errors

    assert errors.get('timezone')
    assert errors.get('tasks')


def test_dag_config_cron_validation():
    invalid_payload = {
        'schedule_interval': '0 *'
    }
    schema = transformation_schema.DagConfigSchema()
    errors = schema.load(invalid_payload).errors

    assert errors.get('schedule_interval')


def test_task_overall_config_validation():
    invalid_payload = {
        'schedule_interval': '0 1 * * *', 'timezone': 'Europe/London',
        'tasks': [
            {

            }
        ]
    }
    schema = transformation_schema.DagConfigSchema()
    errors = schema.load(invalid_payload).errors

    assert errors['tasks'][0].get('identifier')
    assert errors['tasks'][0].get('kind')


def test_task_kind_config_validation():
    invalid_payload = {
        'schedule_interval': '0 1 * * *', 'timezone': 'Europe/London',
        'tasks': [
            {
                'identifier': 'stg_covid19',
                'kind': {'action': 'materialization_invalid', 'target': 'table_invalid'},
            }
        ]
    }
    schema = transformation_schema.DagConfigSchema()
    errors = schema.load(invalid_payload).errors

    assert errors['tasks'][0].get('kind').get('action') == ['Not a valid choice.']
    assert errors['tasks'][0].get('kind').get('target') is None


def test_task_partitioning_config_validation():
    invalid_payload = {
        'schedule_interval': '0 1 * * *', 'timezone': 'Europe/London',
        'tasks': [
            {
                'partitioning': {'field': 'date_invalid', 'data_type': 'date_invalid'}
            }
        ]
    }
    schema = transformation_schema.DagConfigSchema()
    errors = schema.load(invalid_payload).errors

    assert errors['tasks'][0].get('partitioning').get('data_type')
    assert errors['tasks'][0].get('partitioning').get('field')


def test_schema_deserialization():
    payload = {
        'schedule_interval': '0 1 * * *', 'timezone': 'Europe/London',
        'params': {'value_a': [1, 2, 3]},
        'database': 'sandbox',
        'schema': 'dop_sandbox_us',
        'tasks': [
            {
                'identifier': 'stg_covid19', 'schema': 'dop_sandbox_us',
                'kind': {'action': 'materialization', 'target': 'table'},
                'partitioning': {'field': 'date', 'data_type': 'date'},
                'dependencies': [
                    'a', 'b', 'c'
                ]
            }
        ]
    }

    try:
        transformation_schema.load_dag_schema(payload)
    except transformation_schema.InvalidDagConfig as e:
        pytest.fail(f'Should not raise exception InvalidDagConfig, error: {e}')

    dag_config = transformation_schema.load_dag_schema(payload)
    assert isinstance(dag_config, transformation_schema.DagConfig)
    assert isinstance(dag_config.tasks[0], transformation_schema.Task)
    assert isinstance(dag_config.tasks[0].kind, transformation_schema.Kind)
    assert isinstance(dag_config.tasks[0].partitioning, transformation_schema.Partitioning)
    assert dag_config.tasks[0].dependencies == ['a', 'b', 'c']
