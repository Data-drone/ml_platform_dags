from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta
from taxi_load.modules.ingest_data import load_data_to_minio

default_args = {
  'owner': 'brian',
  'depends_on_past': False,
  'start_date': datetime(2021, 5, 25),
  'email': ['bpl.law@gmail.com'],
  'email_on_failure': True,
  'email_on_retry': True,
  'retries': 2,
  'retry_delay': timedelta(minutes=2),
}

dag = DAG(  
    dag_id='load_taxi',
    default_args=default_args,
    catchup=False,
    schedule_interval='0 * * * *'
    )

load_taxi = PythonOperator(
    task_id='load_data_to_minio',
    python_callable=load_data_to_minio,
    op_kwargs={'file_to_load': 'trip data/green_tripdata_2015-01.csv',
                'file_save_name': 'green_tripdata_2015-01.csv',
                'write_path_minio': 'raw_data/green_tripdata_2015-01.csv'},
    dag=dag,
    queue='queue_1'
)

load_delta = SparkSubmitOperator(
    task_id='load_delta_lake',
    conn_id='SPARK_LOCAL_CLUSTER',
    application='taxi_load/modules/delta_ingest.py',
    packages='io.delta:delta-core_2.12:1.0.0,org.apache.hadoop:hadoop-aws:3.2.0',
    name='load_delta_lake',
    execution_timeout=timedelta(minutes=15),
    dag=dag,
    queue='queue_2'
)

load_taxi >> load_delta
