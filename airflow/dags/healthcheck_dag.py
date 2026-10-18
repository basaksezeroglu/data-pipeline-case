# 15 dakikada bir batch ve streaming tutarlilik kontrolu

from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from healthcheck_functions import run_healthcheck


def check(pipeline, **context):
    source = PostgresHook(postgres_conn_id="source_db").get_conn()
    target = PostgresHook(postgres_conn_id="target_db").get_conn()
    failed = run_healthcheck(source, target, pipelines=[pipeline], run_name=f"hc_{pipeline}_{context['run_id']}")
    source.close()
    target.close()

    if failed:
        raise AirflowException(f"Healthcheck hatali: {failed}")


with DAG(
    dag_id="iyzico_healthcheck",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["iyzico"],
) as dag:

    PythonOperator(task_id="check_batch", python_callable=check, op_kwargs={"pipeline": "batch"})
    PythonOperator(task_id="check_streaming", python_callable=check, op_kwargs={"pipeline": "streaming"})
