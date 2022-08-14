import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators import ExecuteQueryFromFileOperator

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
    'catchup': False,
    'email_on_retry': False,
}

dag = DAG('create_tables_dag',
          default_args=default_args,
          description='Create required tables',
          schedule_interval=None
        )

create_tables = ExecuteQueryFromFileOperator(
    task_id="Create_tables",
    dag=dag,
    query_file=f"{os.path.dirname(__file__)}/../create_tables.sql",
    redshift_conn_id="redshift"
)