from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta
from taxi_load.modules.ingest_data import load_data_to_minio

AIRFLOW_PATH = "/root/airflow/ext_dags"

default_args = {
  'owner': 'brian',
  'depends_on_past': False,
  'start_date': datetime(2021, 5, 25),
  'email': ['bpl.law@gmail.com'],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 2,
  'retry_delay': timedelta(minutes=1),
}

pre2015_green = ["green_tripdata_2013-08.csv",
                "green_tripdata_2013-09.csv",
                "green_tripdata_2013-10.csv",
                "green_tripdata_2013-11.csv",
                "green_tripdata_2013-12.csv",
                "green_tripdata_2014-01.csv",
                "green_tripdata_2014-02.csv",
                "green_tripdata_2014-03.csv",
                "green_tripdata_2014-04.csv",
                "green_tripdata_2014-05.csv",
                "green_tripdata_2014-06.csv",
                "green_tripdata_2014-07.csv",
                "green_tripdata_2014-08.csv",
                "green_tripdata_2014-09.csv",
                "green_tripdata_2014-10.csv",
                "green_tripdata_2014-11.csv",
                "green_tripdata_2014-12.csv"]

dag = DAG(  
    dag_id='load_taxi',
    default_args=default_args,
    catchup=False,
    schedule_interval=None
    )

load_pre2015_green = []
for i, file in enumerate(pre2015_green):
    load_pre2015_green.append( PythonOperator(
        task_id='load_data_to_minio_green_pre2015_' + str(i),
        python_callable=load_data_to_minio,
        op_kwargs={'file_to_load': 'trip data/' + file,
                    'file_save_name': file,
                    'write_path_minio': 'raw_data/' + file},
        dag=dag,
        queue='queue_1'
    ))

load_delta_pre2015 = SparkSubmitOperator(
    task_id='load_delta_lake_pre2015',
    conn_id='SPARK_LOCAL_CLUSTER',
    application=AIRFLOW_PATH + '/taxi_load/modules/delta_ingest.py',
    packages='io.delta:delta-core_2.12:1.0.0,org.apache.hadoop:hadoop-aws:3.2.0',
    name='load_delta_lake',
    execution_timeout=timedelta(minutes=15),
    executor_memory='6g',
    num_executors=2,
    #application_args="{0} {1}".format(','.join(pre2015_green), 'green_taxi_pre2015'),
    application_args=['/'.join(pre2015_green), 'green_taxi_pre2015'],
    dag=dag,
    queue='queue_2'
)

for item in load_pre2015_green:
    item >> load_delta_pre2015 

h1_2015_green = ["green_tripdata_2015-01.csv",
                "green_tripdata_2015-02.csv",
                "green_tripdata_2015-03.csv",
                "green_tripdata_2015-04.csv",
                "green_tripdata_2015-05.csv",
                "green_tripdata_2015-06.csv"]

load_2015_h1_green = []
for i, file in enumerate(h1_2015_green):
    load_2015_h1_green.append( PythonOperator(
        task_id='load_data_to_minio_green_2015_h1' + str(i),
        python_callable=load_data_to_minio,
        op_kwargs={'file_to_load': 'trip data/' + file,
                    'file_save_name': file,
                    'write_path_minio': 'raw_data/' + file},
        dag=dag,
        queue='queue_1'
    ))

load_delta_2015_h1 = SparkSubmitOperator(
    task_id='load_delta_lake_2015_h1',
    conn_id='SPARK_LOCAL_CLUSTER',
    application=AIRFLOW_PATH + '/taxi_load/modules/delta_ingest.py',
    packages='io.delta:delta-core_2.12:1.0.0,org.apache.hadoop:hadoop-aws:3.2.0',
    name='load_delta_lake',
    execution_timeout=timedelta(minutes=15),
    executor_memory='6g',
    num_executors=2,
    #application_args="{0} {1}".format(','.join(h1_2015_green), 'green_taxi_2015_h1'),
    application_args=['/'.join(h1_2015_green), 'green_taxi_2015_h1'],
    dag=dag,
    queue='queue_2'
)

for item in load_2015_h1_green:
    item >> load_delta_2015_h1

create_green_merged = SparkSubmitOperator(
    task_id='create_green_merged',
    conn_id='SPARK_LOCAL_CLUSTER',
    application=AIRFLOW_PATH + '/taxi_load/modules/delta_etl_job.py',
    packages='io.delta:delta-core_2.12:1.0.0,org.apache.hadoop:hadoop-aws:3.2.0',
    name='load_delta_lake',
    execution_timeout=timedelta(minutes=15),
    executor_memory='6g',
    num_executors=2,
    dag=dag,
    queue='queue_2'
)

load_delta_pre2015 >> create_green_merged
load_delta_2015_h1 >> create_green_merged