from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipeline import load_postgres, transform_hdfs, load_clickhouse, verify

with DAG('gilach_citybikes',start_date=pendulum.datetime(2025,5,12,tz='UTC'),
         schedule=None,catchup=False,max_active_runs=1,
         default_args={'owner':'student','retries':1,'retry_delay':timedelta(seconds=20)},
         tags=['simulative','diploma']) as dag:
    raw=PythonOperator(task_id='json_to_postgres',python_callable=load_postgres)
    mart=PythonOperator(task_id='spark_to_hdfs',python_callable=transform_hdfs)
    publish=PythonOperator(task_id='hdfs_to_clickhouse',python_callable=load_clickhouse)
    checks=PythonOperator(task_id='quality_checks',python_callable=verify)
    raw >> mart >> publish >> checks
