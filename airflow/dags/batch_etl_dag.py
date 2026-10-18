# Gunluk batch ETL
# kaynak (postgreSource) -> stg -> ods -> dwh
#
# start_run -> extract_customers / extract_orders -> load_ods_customers / load_ods_orders
#           -> load_dwh_customers -> load_dwh_orders -> end_run -> healthcheck

import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

from healthcheck_functions import run_healthcheck

SQL_FOLDER = os.path.join(os.path.dirname(__file__), "sql")

# kaynaktan cekilecek tablolar ve kolonlari (udate hep son kolon)
TABLES = {
    "customers": ["id", "name", "email", "idate", "udate"],
    "orders": ["id", "customer_id", "amount", "status", "idate", "udate"],
}


# baglantilar docker-compose'daki AIRFLOW_CONN_SOURCE_DB / AIRFLOW_CONN_TARGET_DB'den geliyor
def get_source_conn():
    return PostgresHook(postgres_conn_id="source_db").get_conn()


def get_target_conn():
    return PostgresHook(postgres_conn_id="target_db").get_conn()


def get_dates(context):
    # start_run'in dondurdugu {etl_date}
    return context["ti"].xcom_pull(task_ids="start_run")


def start_run(**context):
    conn = get_target_conn()
    cur = conn.cursor()

    cur.execute("""
        insert into etl.run_log (run_id, etl_date) values (%s, current_date)
        on conflict (run_id) do update
            set etl_date = current_date, status = 'running', started_at = now(), finished_at = null
        returning etl_date
    """, (context["run_id"],))
    etl_date = cur.fetchone()[0]

    conn.commit()
    conn.close()

    print(f"etl_date={etl_date}")
    return {"etl_date": str(etl_date)}


def extract_to_stg(table, **context):
    etl_date = get_dates(context)["etl_date"]
    columns = ", ".join(TABLES[table])

    source = get_source_conn()
    target = get_target_conn()
    source_cur = source.cursor()
    target_cur = target.cursor()

    # watermark: en son hangi tarihe kadar veri cektik
    target_cur.execute("select last_udate from etl.watermark where table_name = %s", (table,))
    watermark = target_cur.fetchone()[0]

    # o tarihten sonra eklenen (idate) veya guncellenen (udate) kayitlar.
    # >= kullaniyoruz ki tam sinirdaki kayit kacmasin; ayni kayit tekrar gelse de
    # dwh'a id'ye gore sil-yukle yaptigimiz icin sorun olmuyor.
    source_cur.execute(f"select {columns} from public.{table} where idate >= %s or udate >= %s",
                       (watermark, watermark))
    rows = source_cur.fetchall()

    target_cur.execute(f"truncate table stg.{table}")
    if rows:
        execute_values(target_cur,
                       f"insert into stg.{table} ({columns}, etl_date) values %s",
                       [row + (etl_date,) for row in rows])
    target.commit()

    source.close()
    target.close()

    max_udate = max(row[-1] for row in rows) if rows else None
    print(f"{table}: {len(rows)} yeni/guncellenen kayit stg'ye yazildi")
    return {"row_count": len(rows), "max_udate": str(max_udate) if max_udate else None}


def run_sql_file(file_name, **context):
    with open(os.path.join(SQL_FOLDER, file_name), encoding="utf-8") as f:
        sql = f.read()

    conn = get_target_conn()
    conn.cursor().execute(sql, get_dates(context))
    conn.commit()
    conn.close()
    print(f"{file_name} calisti")


def end_run(**context):
    dates = get_dates(context)
    conn = get_target_conn()
    cur = conn.cursor()

    stats = {}
    for table in TABLES:
        result = context["ti"].xcom_pull(task_ids=f"extract_{table}")
        stats[table] = result["row_count"]

        # watermark'i en sonda guncelliyoruz.
        # arada bir adim hata alirsa watermark ilerlemez, sonraki run ayni kayitlari tekrar ceker.
        if result["max_udate"]:
            cur.execute("""
                update etl.watermark
                set last_udate = %s, last_etl_date = %s, updated_at = now()
                where table_name = %s
            """, (result["max_udate"], dates["etl_date"], table))

    cur.execute("update etl.run_log set status = 'success', finished_at = now(), stats = %s where run_id = %s",
                (json.dumps(stats), context["run_id"]))
    conn.commit()
    conn.close()


def healthcheck(**context):
    source = get_source_conn()
    target = get_target_conn()
    failed = run_healthcheck(source, target, pipelines=["batch"], run_name=f"etl_{context['run_id']}")
    source.close()
    target.close()

    if failed:
        raise AirflowException(f"Healthcheck hatali: {failed}")


def mark_run_failed(context):
    # bir task hata alirsa run_log'da failed yap
    conn = get_target_conn()
    conn.cursor().execute("update etl.run_log set status = 'failed', finished_at = now() "
                          "where run_id = %s and status = 'running'", (context["run_id"],))
    conn.commit()
    conn.close()


default_args = {
    "owner": "basak",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": mark_run_failed,
}

with DAG(
    dag_id="iyzico_batch_etl",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,   # ayni anda iki run calisip watermark'i bozmasin
    default_args=default_args,
    tags=["iyzico"],
) as dag:

    start = PythonOperator(task_id="start_run", python_callable=start_run)

    extract_customers = PythonOperator(task_id="extract_customers", python_callable=extract_to_stg,
                                       op_kwargs={"table": "customers"})
    extract_orders = PythonOperator(task_id="extract_orders", python_callable=extract_to_stg,
                                    op_kwargs={"table": "orders"})

    ods_customers = PythonOperator(task_id="load_ods_customers", python_callable=run_sql_file,
                                   op_kwargs={"file_name": "stg_to_ods_customers.sql"})
    ods_orders = PythonOperator(task_id="load_ods_orders", python_callable=run_sql_file,
                                op_kwargs={"file_name": "stg_to_ods_orders.sql"})

    dwh_customers = PythonOperator(task_id="load_dwh_customers", python_callable=run_sql_file,
                                   op_kwargs={"file_name": "ods_to_dwh_customers.sql"})
    dwh_orders = PythonOperator(task_id="load_dwh_orders", python_callable=run_sql_file,
                                op_kwargs={"file_name": "ods_to_dwh_orders.sql"})

    end = PythonOperator(task_id="end_run", python_callable=end_run)
    check = PythonOperator(task_id="healthcheck", python_callable=healthcheck, retries=0)

    start >> [extract_customers, extract_orders]
    extract_customers >> ods_customers
    extract_orders >> ods_orders
    [ods_customers, ods_orders] >> dwh_customers >> dwh_orders >> end >> check
