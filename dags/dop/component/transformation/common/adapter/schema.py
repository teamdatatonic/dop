import typing
import copy

from dataclasses import dataclass
from croniter import croniter
from typing import List, Optional
from decimal import Decimal

from marshmallow import validate, post_load, Schema, fields

TASK_KIND_MATERI = 'materialization'
TASK_KIND_ASSERT = 'assertion'
TASK_KIND_INVOKE = 'invocation'
TASK_KIND_DBT = 'dbt'

NATIVE_TASK_KIND = [TASK_KIND_MATERI, TASK_KIND_ASSERT, TASK_KIND_INVOKE]
CUSTOM_TASK_KIND = [TASK_KIND_DBT]


def dbt_argument_validation_mapper(option, value):
    allowed_options = ['-m', '-x', '--fail-fast', '--threads', '--exclude', '--full-refresh']
    if option not in allowed_options:
        raise DbtTaskException(
            f'Supported DBT command line argument options are: {allowed_options}, `{option}` supplied'
        )


def dbt_validation_func(task):
    allowed_options = ['run', 'test']
    if task.kind.target not in allowed_options:
        raise DbtTaskException(f'DBT task.kind.target must be one of {allowed_options}, `{task.kind.target}` supplied')

    # check version
    dbt_version = task.options.get('version')
    if not dbt_version:
        raise DbtTaskException('DBT version must be supplied in the configuration')

    v_major, v_minor, v_patch = dbt_version.split('.')
    if int(v_major) < 0 or Decimal(f'{v_minor}.{v_patch}') < Decimal('18.1'):
        raise DbtTaskException(f'DBT version must be >= 0.18.1, {dbt_version} is supplied')

    # check DBT arguments, only allow certain arguments to be used
    arguments = task.options.get('arguments')

    if arguments:
        for argument in arguments:
            dbt_argument_validation_mapper(option=argument.get('option'), value=argument.get('value'))


def materialization_validation_func(task):
    allowed_options = ['table', 'view', 'udf', 'stored_procedure', 'schema']
    if task.kind.target not in allowed_options:
        raise MaterializationTaskException(
            f'Materialization task.kind.target must be one of {allowed_options}, `{task.kind.target}`supplied'
        )


def assertion_validation_func(task):
    allowed_options = ['assertion', 'assertion_sensor']
    if task.kind.target not in allowed_options:
        raise AssertionTaskException(
            f'Assertion task.kind.target must be one of {allowed_options}, {task.kind.target} supplied'
        )


def invocation_validation_func(task):
    allowed_options = ['stored_procedure']
    if task.kind.target not in allowed_options:
        raise InvocationTaskException(
            f'Invocation task.kind.target must be one of {allowed_options}, {task.kind.target} supplied'
        )


def data_validation_mapper(task):
    if task.kind.action == TASK_KIND_DBT:
        return dbt_validation_func
    elif task.kind.action == TASK_KIND_MATERI:
        return materialization_validation_func
    elif task.kind.action == TASK_KIND_ASSERT:
        return assertion_validation_func
    elif task.kind.action == TASK_KIND_INVOKE:
        return invocation_validation_func

    return None


class InvalidDagConfig(ValueError):
    pass


class DbtTaskException(InvalidDagConfig):
    pass


class MaterializationTaskException(InvalidDagConfig):
    pass


class AssertionTaskException(InvalidDagConfig):
    pass


class InvocationTaskException(InvalidDagConfig):
    pass


class IsValidCron(validate.Validator):
    default_message = 'Not a valid Cron Expression'

    def __call__(self, value) -> typing.Any:
        message = f'The schedule_interval expression `{value}` must be a valid CRON expression: ' \
                  'validate it here https://crontab.guru/'
        if not croniter.is_valid(value):
            raise validate.ValidationError(message)

        return value


@dataclass
class Partitioning:
    field: str
    data_type: str


class PartitioningSchema(Schema):
    field = fields.String(validate=validate.OneOf(['date']))
    data_type = fields.String(validate=validate.OneOf(['timestamp', 'datatime', 'date']))


@dataclass
class Kind:
    action: str
    target: str


class KindSchema(Schema):
    action = fields.String(
        validate=validate.OneOf([TASK_KIND_MATERI, TASK_KIND_ASSERT, TASK_KIND_INVOKE, TASK_KIND_DBT])
    )
    target = fields.String()


@dataclass
class Task:
    kind: Kind
    database: Optional[str]
    schema: Optional[str]
    identifier: str
    partitioning: Optional[Partitioning]
    dependencies: List[str]
    options: dict


class TaskSchema(Schema):
    kind = fields.Nested(KindSchema, required=True)
    database = fields.String(required=False)
    schema = fields.String(required=False)
    identifier = fields.String(required=True)
    partitioning = fields.Nested(PartitioningSchema, required=False, missing=None)
    dependencies = fields.List(cls_or_instance=fields.Str(), required=False, missing=[])
    options = fields.Dict(required=False, missing={})


@dataclass
class DagConfig:
    enabled: bool
    timezone: str
    schedule_interval: str
    params: Optional[dict]
    database: str
    schema: str
    tasks: List[Task]


class DagConfigSchema(Schema):
    enabled = fields.Bool(required=False, missing=True)
    timezone = fields.Str(required=True)
    schedule_interval = fields.Str(validate=IsValidCron(), missing=None)
    params = fields.Dict(missing={})
    database = fields.Str(required=True)
    schema = fields.Str(required=True)
    tasks = fields.List(cls_or_instance=fields.Nested(TaskSchema), required=True)

    @post_load
    def make_dag_config(self, data, **kwargs):
        data_with_objects = copy.deepcopy(data)
        tasks = []
        for task in data_with_objects['tasks']:
            task['kind'] = Kind(**task['kind'])
            task['database'] = task['database'] if 'database' in task else data_with_objects['database']
            task['schema'] = task['schema'] if 'schema' in task else data_with_objects['schema']
            task['partitioning'] = Partitioning(**task['partitioning']) if task['partitioning'] is not None \
                else task['partitioning']

            task_entity = Task(**task)

            # Additional validation
            validation_func = data_validation_mapper(task=task_entity)
            if validation_func:
                validation_func(task_entity)

            tasks.append(task_entity)

        data_with_objects['tasks'] = tasks
        return DagConfig(**data_with_objects)


def load_dag_schema(payload) -> DagConfig:
    schema = DagConfigSchema()
    result = schema.load(payload)

    if result.errors:
        raise InvalidDagConfig(result.errors)

    return result.data
