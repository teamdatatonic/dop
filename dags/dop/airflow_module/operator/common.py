from airflow.operators.python_operator import PythonOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator


class BasePythonOperator(PythonOperator):

    def __init__(
            self,
            python_callable,
            op_args=None,
            op_kwargs=None,
            provide_context=False,
            templates_dict=None,
            templates_exts=None,
            *args,
            **kwargs
    ):
        if kwargs.get('priority_weight') is None:
            kwargs['priority_weight'] = 1

        super(BasePythonOperator, self).__init__(
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            provide_context=provide_context,
            templates_dict=templates_dict,
            templates_exts=templates_exts,
            *args,
            **kwargs
        )


class AbstractBaseSensorOperator(BaseSensorOperator):
    def __init__(self, *args, **kwargs):
        super(AbstractBaseSensorOperator, self).__init__(
            *args,
            **kwargs
        )


class TransformationOperator(BasePythonOperator):
    template_fields = ('action', 'target', 'database', 'schema', 'identifier', 'arguments', 'sql', 'templates_dict')

    def __init__(
            self,
            python_callable,
            op_args=None,
            op_kwargs=None,
            provide_context=False,
            templates_dict=None,
            templates_exts=None,
            *args,
            **kwargs
    ):
        super(TransformationOperator, self).__init__(
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            provide_context=provide_context,
            templates_dict=templates_dict,
            templates_exts=templates_exts,
            *args,
            **kwargs
        )
        task = op_kwargs['task']
        self.action = task.kind.action
        self.target = task.kind.target
        self.database = task.database
        self.schema = task.schema
        self.identifier = task.identifier
        self.arguments = task.options.get('arguments')
        self.sql = templates_dict['sql']


class MaterializationOperator(TransformationOperator):
    ui_color = '#f2fade'


class RecreateSpOperator(TransformationOperator):
    ui_color = '#a0c6d9'


class InvocationOperator(TransformationOperator):
    ui_color = '#99cee8'


class AssertOperator(BasePythonOperator):
    ui_color = '#fcc2a7'
    template_fields = ('assertion_sql', 'templates_dict')

    def __init__(
            self,
            python_callable,
            provide_context=False,
            templates_dict=None,
            *args,
            **kwargs
    ):
        super(AssertOperator, self).__init__(
            python_callable=python_callable,
            provide_context=provide_context,
            templates_dict=templates_dict,
            *args,
            **kwargs
        )
        self.assertion_sql = templates_dict['sql']
