from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.docker_operator import DockerOperator
import os

AIRFLOW_PATH = "/root/airflow/ext_dags"

default_args = {
  'owner'              : 'brian',
  'description'        : 'Dag to run an amundsen ingest of my delta lake',
  'depends_on_past'    : False,
  'start_date'         : datetime(2021, 5, 25),
  'email'              : [''],
  'email_on_failure'   : False,
  'email_on_retry'     : False,
  'retries'            : 1,
  'retry_delay'        : timedelta(minutes=1),
}

dag = DAG(  
    dag_id='load_metadata_to_amundsen',
    default_args=default_args,
    catchup=False,
    schedule_interval=None
    )

##### Variables for cluster endpoints
neo4j_endpoint = 'bolt://neo4j_amundsen:7687/'
neo4j_user = 'neo4j'
neo4j_password = 'test'

### create the environment variables for the docker operator


start_dag = DummyOperator(
        task_id='start_dag',
        dag=dag
        )

ingest_base_data = DockerOperator(
  task_id= 'ingest_metadata_from_deltalake',
  image='amundsen_load',
  container_name='task__amundsen_ingest_metadata',
  api_version='auto',
  auto_remove=True,
  docker_url='unix://var/run/docker.sock',
  network_mode="datalake_ml_platform",
  queue='queue_1',
  dag=dag
)

update_elasticsearch = DockerOperator(
  task_id= 'update_elastisearch',
  image='amundsen_load',
  container_name='task__amundsen_update_elastisearch',
  api_version='auto',
  auto_remove=True,
  docker_url='unix://var/run/docker.sock',
  entrypoint=["/scripts/modules/load_env_run_script.sh", "/scripts/modules/update_elasticsearch.py"],
  network_mode="datalake_ml_platform",
  queue='queue_1',
  dag=dag
)

end_dag = DummyOperator(
        task_id='end_dag',
        dag=dag
        )        

start_dag >> ingest_base_data >> update_elasticsearch >> end_dag