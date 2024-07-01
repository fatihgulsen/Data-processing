import logging
import os
import sys
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)
os.environ['PYTHONPATH'] = "/opt/airflow/scripts"

# DAG Configuration
default_args = {
    "owner": "airflow",
    "retry_delay": timedelta(minutes=5),
    "retry":0
}

# scripts dizinini sistem yoluna ekle
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

@dag(
    "costa_rica_data_processing",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["costa_rica"],
)
def costa_rica_processing():
    @task
    def get_unprocessed_tables():
        hook = PostgresHook(postgres_conn_id="new_data_postgres")
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema='cr' AND table_type='BASE TABLE'
            AND table_name NOT IN (SELECT table_name FROM cr.processed_tables);
            """
        )
        unprocessed_tables = cursor.fetchone()
        cursor.close()
        conn.close()
        return unprocessed_tables

    @task
    def mark_table_as_processed(table_name):
        conn = PostgresHook(postgres_conn_id="new_data_postgres").get_conn()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO processed_tables (table_name) VALUES (%s) ON CONFLICT (table_name) DO NOTHING;",
            (table_name,),
        )
        conn.commit()
        cursor.close()
        conn.close()

    def submit_spark_job(task_id, script, table_name):
        logger.info(f"PYTHONPATH:{sys.path}", )
        return SparkSubmitOperator(
            task_id=task_id,
            application=f"/opt/airflow/scripts/dags/cr/{script}",
            application_args=table_name,
            executor_memory="2g",
            executor_cores=2,
            num_executors=1,
            driver_memory="1g",
            trigger_rule="all_success",
            conf={
            'spark.executorEnv.PYTHONPATH': '/opt/airflow/scripts',
            'spark.yarn.appMasterEnv.PYTHONPATH': '/opt/airflow/scripts'
        },
            jars="/opt/airflow/scripts/jars/postgresql-42.2.24.jar"
        )

    table_name = get_unprocessed_tables()
    print("PYTHONPATH:", sys.path)

    process = submit_spark_job("process", "process.py", table_name)

    mark_processed = mark_table_as_processed(table_name)

    process >> mark_processed


costa_rica_dag = costa_rica_processing()
