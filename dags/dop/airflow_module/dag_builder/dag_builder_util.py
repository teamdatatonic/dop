from datetime import timedelta, datetime

from airflow import DAG
from airflow.models import Variable


def get_default_dag_start_date(tzinfo):
    return datetime(1970, 1, 1, tzinfo=tzinfo)  # You cannot back fill a dag prior to this date


def create_dag(
        dag_id,
        start_date,
        schedule_interval=None,
        retries=3,
        retry_delay=None,
        owner='airflow',
        depends_on_past=False,
        catchup=False,
        max_active_runs=5,
        concurrency=int(Variable.get("DAG_CONCURRENCY", default_var=2)),
        template_searchpath=None,
        user_defined_macros=None
):
    default_args = {
        'owner': owner,
        'depends_on_past': depends_on_past,
        'start_date': start_date,
        'retries': retries,
        'retry_delay': retry_delay if retry_delay is not None else timedelta(minutes=5)
    }

    return DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule_interval,
        catchup=catchup,
        max_active_runs=max_active_runs,
        concurrency=concurrency,
        template_searchpath=template_searchpath,
        user_defined_macros=user_defined_macros
    )
