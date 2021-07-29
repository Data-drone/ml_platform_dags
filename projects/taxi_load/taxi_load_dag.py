from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta
from taxi_load.modules.ingest_to_minio import load_data_to_minio

AIRFLOW_PATH = "/root/airflow/ext_dags"

default_args = {
  'owner': 'brian',
  'depends_on_past': False,
  'start_date': datetime(2021, 5, 25),
  'email': [''],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 2,
  'retry_delay': timedelta(minutes=1),
}


############ pre2015 - Green #######################

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
    task_id='load_raw_zone_green_pre2015',
    name='load_green_pre2015_to_raw',
    conn_id='SPARK_LOCAL_CLUSTER',
    application=AIRFLOW_PATH + '/taxi_load/modules/ingest_raw_to_delta.py',
    py_files=AIRFLOW_PATH + '/taxi_load/utils/spark_setup.py',
    packages='io.delta:delta-core_2.12:1.0.0,org.apache.hadoop:hadoop-aws:3.2.0',
    execution_timeout=timedelta(minutes=1),
    executor_cores=4,
    executor_memory='6g',
    driver_memory='6g',
    num_executors=2,
    #application_args="{0} {1}".format(','.join(pre2015_green), 'green_taxi_pre2015'),
    application_args=['/'.join(pre2015_green), 'green_taxi_pre2015'],
    dag=dag,
    queue='queue_2'
)

for item in load_pre2015_green:
    item >> load_delta_pre2015 

############ 2015_h1 - Green #######################

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
    task_id='load_raw_zone_green_2015h1',
    name='load_raw_zone_green_2015h1',
    conn_id='SPARK_LOCAL_CLUSTER',
    application=AIRFLOW_PATH + '/taxi_load/modules/ingest_raw_to_delta.py',
    py_files=AIRFLOW_PATH + '/taxi_load/utils/spark_setup.py',
    packages='io.delta:delta-core_2.12:1.0.0,org.apache.hadoop:hadoop-aws:3.2.0',
    executor_memory='6g',
    executor_cores=4,
    num_executors=2,
    #application_args="{0} {1}".format(','.join(h1_2015_green), 'green_taxi_2015_h1'),
    application_args=['/'.join(h1_2015_green), 'green_taxi_2015_h1'],
    dag=dag,
    queue='queue_2'
)

for item in load_2015_h1_green:
    item >> load_delta_2015_h1

############ 2015-2016_h1 - Green #######################

h2_2015_2016_h1_green = [
    "green_tripdata_2015-07.csv",
    "green_tripdata_2015-08.csv",
    "green_tripdata_2015-09.csv",
    "green_tripdata_2015-10.csv",
    "green_tripdata_2015-11.csv",
    "green_tripdata_2015-12.csv",
    "green_tripdata_2016-01.csv",
    "green_tripdata_2016-02.csv",
    "green_tripdata_2016-03.csv",
    "green_tripdata_2016-04.csv",
    "green_tripdata_2016-05.csv",
    "green_tripdata_2016-06.csv"
]

load_h2_2015_2016_h1_green = []
for i, file in enumerate(h2_2015_2016_h1_green):
    load_h2_2015_2016_h1_green.append( PythonOperator(
        task_id='load_data_to_minio_green_2015_h2-2016-h1' + str(i),
        python_callable=load_data_to_minio,
        op_kwargs={'file_to_load': 'trip data/' + file,
                    'file_save_name': file,
                    'write_path_minio': 'raw_data/' + file},
        dag=dag,
        queue='queue_1'
    ))

load_delta_h2_2015_2016_h1_green = SparkSubmitOperator(
    task_id='load_raw_zone_green_2015h2_2016_h1',
    name='load_raw_zone_green_2015h2_2016_h1',
    conn_id='SPARK_LOCAL_CLUSTER',
    application=AIRFLOW_PATH + '/taxi_load/modules/ingest_raw_to_delta.py',
    py_files=AIRFLOW_PATH + '/taxi_load/utils/spark_setup.py',
    packages='io.delta:delta-core_2.12:1.0.0,org.apache.hadoop:hadoop-aws:3.2.0',
    executor_memory='6g',
    executor_cores=4,
    num_executors=2,
    #application_args="{0} {1}".format(','.join(h1_2015_green), 'green_taxi_2015_h1'),
    application_args=['/'.join(h2_2015_2016_h1_green), 'green_taxi_2015_h2_2016_h1'],
    dag=dag,
    queue='queue_2'
)

for item in load_h2_2015_2016_h1_green:
    item >> load_delta_h2_2015_2016_h1_green

##################################################################

create_green_merged = SparkSubmitOperator(
    task_id='create_green_merged',
    conn_id='SPARK_LOCAL_CLUSTER',
    application=AIRFLOW_PATH + '/taxi_load/modules/merge_raw_green.py',
    py_files=AIRFLOW_PATH + '/taxi_load/utils/spark_setup.py',
    packages='io.delta:delta-core_2.12:1.0.0,org.apache.hadoop:hadoop-aws:3.2.0',
    name='create_green_merged',
    #execution_timeout=timedelta(minutes=5),
    driver_memory='6g',
    executor_memory='6g',
    num_executors=2,
    dag=dag,
    queue='queue_2'
)

load_delta_pre2015 >> create_green_merged
load_delta_2015_h1 >> create_green_merged
load_delta_h2_2015_2016_h1_green >> create_green_merged

############ pre 2015 - Yellow #######################

# there is older yellow data too
pre2015_yellow = ["yellow_tripdata_2013-08.csv",
                "yellow_tripdata_2013-09.csv",
                "yellow_tripdata_2013-10.csv",
                "yellow_tripdata_2013-11.csv",
                "yellow_tripdata_2013-12.csv",
                "yellow_tripdata_2014-01.csv",
                "yellow_tripdata_2014-02.csv",
                "yellow_tripdata_2014-03.csv",
                "yellow_tripdata_2014-04.csv",
                "yellow_tripdata_2014-05.csv",
                "yellow_tripdata_2014-06.csv",
                "yellow_tripdata_2014-07.csv",
                "yellow_tripdata_2014-08.csv",
                "yellow_tripdata_2014-09.csv",
                "yellow_tripdata_2014-10.csv",
                "yellow_tripdata_2014-11.csv",
                "yellow_tripdata_2014-12.csv"]

load_pre2015_yellow = []
for i, file in enumerate(pre2015_yellow):
    load_pre2015_yellow.append( PythonOperator(
        task_id='load_data_to_minio_yellow_pre2015_' + str(i),
        python_callable=load_data_to_minio,
        op_kwargs={'file_to_load': 'trip data/' + file,
                    'file_save_name': file,
                    'write_path_minio': 'raw_data/' + file},
        dag=dag,
        queue='queue_1'
    ))

load_delta_pre_2015_yellow = SparkSubmitOperator(
    task_id='load_raw_zone_yellow_pre2015',
    conn_id='SPARK_LOCAL_CLUSTER',
    application=AIRFLOW_PATH + '/taxi_load/modules/ingest_raw_to_delta.py',
    py_files=AIRFLOW_PATH + '/taxi_load/utils/spark_setup.py',
    packages='io.delta:delta-core_2.12:1.0.0,org.apache.hadoop:hadoop-aws:3.2.0',
    name='load_raw_zone_yellow_pre2015',
    execution_timeout=timedelta(minutes=15),
    driver_memory='8g',
    executor_memory='8g',
    executor_cores=4,
    num_executors=2,
    #application_args="{0} {1}".format(','.join(h1_2015_green), 'green_taxi_2015_h1'),
    application_args=['/'.join(pre2015_yellow), 'yellow_taxi_pre2015'],
    dag=dag,
    queue='queue_2'
)

for item in load_pre2015_yellow:
    item >> load_delta_pre_2015_yellow

############ 2015-2016_h1 - Yellow #######################

all_2015_2016_h1_yellow = [
    "yellow_tripdata_2015-01.csv",
    "yellow_tripdata_2015-02.csv",
    "yellow_tripdata_2015-03.csv",
    "yellow_tripdata_2015-04.csv",
    "yellow_tripdata_2015-05.csv",
    "yellow_tripdata_2015-06.csv",
    "yellow_tripdata_2015-07.csv",
    "yellow_tripdata_2015-08.csv",
    "yellow_tripdata_2015-09.csv",
    "yellow_tripdata_2015-10.csv",
    "yellow_tripdata_2015-11.csv",
    "yellow_tripdata_2015-12.csv",
    "yellow_tripdata_2016-01.csv",
    "yellow_tripdata_2016-02.csv",
    "yellow_tripdata_2016-03.csv",
    "yellow_tripdata_2016-04.csv",
    "yellow_tripdata_2016-05.csv",
    "yellow_tripdata_2016-06.csv"
]

load_all_2015_2016_h1_yellow = []
for i, file in enumerate(all_2015_2016_h1_yellow):
    load_all_2015_2016_h1_yellow.append( PythonOperator(
        task_id='load_data_to_minio_yellow_2015_2016_h1' + str(i),
        python_callable=load_data_to_minio,
        op_kwargs={'file_to_load': 'trip data/' + file,
                    'file_save_name': file,
                    'write_path_minio': 'raw_data/' + file},
        dag=dag,
        queue='queue_1'
    ))

load_delta_h2_2015_2016_h1_yellow = SparkSubmitOperator(
    task_id='load_raw_zone_yellow_h2_2015_2016_h1',
    name='load_raw_zone_yellow_h2_2015_2016_h1',
    conn_id='SPARK_LOCAL_CLUSTER',
    application=AIRFLOW_PATH + '/taxi_load/modules/ingest_raw_to_delta.py',
    py_files=AIRFLOW_PATH + '/taxi_load/utils/spark_setup.py',
    packages='io.delta:delta-core_2.12:1.0.0,org.apache.hadoop:hadoop-aws:3.2.0',
    execution_timeout=timedelta(minutes=15),
    driver_memory='8g',
    executor_memory='8g',
    executor_cores=4,
    num_executors=2,
    #application_args="{0} {1}".format(','.join(h1_2015_green), 'green_taxi_2015_h1'),
    application_args=['/'.join(all_2015_2016_h1_yellow), 'yellow_taxi_2015_2016_h1'],
    dag=dag,
    queue='queue_2'
)

for item in load_all_2015_2016_h1_yellow:
    item >> load_delta_h2_2015_2016_h1_yellow

################################################################

create_yellow_merged = SparkSubmitOperator(
    task_id='create_yellow_merged',
    conn_id='SPARK_LOCAL_CLUSTER',
    application=AIRFLOW_PATH + '/taxi_load/modules/merge_raw_yellow.py',
    py_files=AIRFLOW_PATH + '/taxi_load/utils/spark_setup.py',
    packages='io.delta:delta-core_2.12:1.0.0,org.apache.hadoop:hadoop-aws:3.2.0',
    name='create_yellow_merged',
    #execution_timeout=timedelta(minutes=5),
    driver_memory='6g',
    executor_memory='6g',
    num_executors=2,
    dag=dag,
    queue='queue_2'
)


load_delta_pre_2015_yellow >> create_yellow_merged
load_delta_h2_2015_2016_h1_yellow >> create_yellow_merged

clean_green = SparkSubmitOperator(
    task_id='generate_clean_green_table',
    conn_id='SPARK_LOCAL_CLUSTER',
    application=AIRFLOW_PATH + '/taxi_load/modules/clean_green_merged.py',
    py_files=AIRFLOW_PATH + '/taxi_load/utils/spark_setup.py',
    packages='io.delta:delta-core_2.12:1.0.0,org.apache.hadoop:hadoop-aws:3.2.0',
    name='generate_clean_green_table',
    execution_timeout=timedelta(minutes=15),
    driver_memory='8g',
    executor_memory='8g',
    executor_cores=4,
    num_executors=3,
    #application_args="{0} {1}".format(','.join(h1_2015_green), 'green_taxi_2015_h1'),
    dag=dag,
    queue='queue_2'
)

clean_yellow = SparkSubmitOperator(
    task_id='generate_clean_yellow_table',
    conn_id='SPARK_LOCAL_CLUSTER',
    application=AIRFLOW_PATH + '/taxi_load/modules/clean_yellow_merged.py',
    py_files=AIRFLOW_PATH + '/taxi_load/utils/spark_setup.py',
    packages='io.delta:delta-core_2.12:1.0.0,org.apache.hadoop:hadoop-aws:3.2.0',
    name='generate_clean_yellow_table',
    execution_timeout=timedelta(minutes=15),
    driver_memory='8g',
    executor_memory='8g',
    executor_cores=4,
    num_executors=3,
    #application_args="{0} {1}".format(','.join(h1_2015_green), 'green_taxi_2015_h1'),
    dag=dag,
    queue='queue_2'
)

create_green_merged >> clean_green
create_yellow_merged >> clean_yellow

merged_data = SparkSubmitOperator(
    task_id='generate_processed_taxi_table',
    name='generate_processed_taxi_table',
    conn_id='SPARK_LOCAL_CLUSTER',
    application=AIRFLOW_PATH + '/taxi_load/modules/merge_sources.py',
    py_files=AIRFLOW_PATH + '/taxi_load/utils/spark_setup.py',
    packages='io.delta:delta-core_2.12:1.0.0,org.apache.hadoop:hadoop-aws:3.2.0',
    execution_timeout=timedelta(minutes=15),
    driver_memory='8g',
    executor_memory='8g',
    executor_cores=4,
    num_executors=3,
    #application_args="{0} {1}".format(','.join(h1_2015_green), 'green_taxi_2015_h1'),
    dag=dag,
    queue='queue_2'
)

clean_green >> merged_data
clean_yellow >> merged_data
