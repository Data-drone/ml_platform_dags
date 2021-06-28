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
network = 'ml_platform'

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
  entrypoint=["/scripts/modules/load_env_run_script.sh", "/scripts/modules/ingest_delta_base_metadata.py --schema raw --schema clean --schema processed"],
  network_mode=network,
  queue='queue_1',
  dag=dag
)

update_high_watermark_raw = DockerOperator(
  task_id= 'update_raw_high_watermark',
  image='amundsen_load',
  container_name='task__amundsen_update_raw_high_wm',
  api_version='auto',
  auto_remove=True,
  docker_url='unix://var/run/docker.sock',
  entrypoint=["/scripts/modules/load_env_run_script.sh", "/scripts/modules/calculate_delta_wm_metadata.py --agg_func max --watermark_type high_watermark --database default --schema raw"],
  #command="--agg_func max --watermark_type high_watermark",
  network_mode="ml_platform",
  queue='queue_1',
  dag=dag
)

update_low_watermark_raw = DockerOperator(
  task_id= 'update_raw_low_watermark',
  image='amundsen_load',
  container_name='task__amundsen_update_raw_low_wm',
  api_version='auto',
  auto_remove=True,
  docker_url='unix://var/run/docker.sock',
  entrypoint=["/scripts/modules/load_env_run_script.sh", "/scripts/modules/calculate_delta_wm_metadata.py --agg_func min --watermark_type low_watermark --database default --schema raw"],
  #command=["min", "low_watermark"],
  network_mode="ml_platform",
  queue='queue_1',
  dag=dag
)

update_high_watermark_clean = DockerOperator(
  task_id= 'update_clean_high_watermark',
  image='amundsen_load',
  container_name='task__amundsen_update_clean_high_wm',
  api_version='auto',
  auto_remove=True,
  docker_url='unix://var/run/docker.sock',
  entrypoint=["/scripts/modules/load_env_run_script.sh", "/scripts/modules/calculate_delta_wm_metadata.py --agg_func max --watermark_type high_watermark --database default --schema clean"],
  #command="--agg_func max --watermark_type high_watermark",
  network_mode="ml_platform",
  queue='queue_1',
  dag=dag
)

update_low_watermark_clean = DockerOperator(
  task_id= 'update_clean_low_watermark',
  image='amundsen_load',
  container_name='task__amundsen_update_clean_low_wm',
  api_version='auto',
  auto_remove=True,
  docker_url='unix://var/run/docker.sock',
  entrypoint=["/scripts/modules/load_env_run_script.sh", "/scripts/modules/calculate_delta_wm_metadata.py --agg_func min --watermark_type low_watermark --database default --schema clean"],
  #command=["min", "low_watermark"],
  network_mode="ml_platform",
  queue='queue_1',
  dag=dag
)

update_high_watermark_processed = DockerOperator(
  task_id= 'update_processed_high_watermark',
  image='amundsen_load',
  container_name='task__amundsen_update_processed_highwatermark',
  api_version='auto',
  auto_remove=True,
  docker_url='unix://var/run/docker.sock',
  entrypoint=["/scripts/modules/load_env_run_script.sh", "/scripts/modules/calculate_delta_wm_metadata.py --agg_func max --watermark_type high_watermark --database default --schema processed"],
  #command="--agg_func max --watermark_type high_watermark",
  network_mode="ml_platform",
  queue='queue_1',
  dag=dag
)

update_low_watermark_processed = DockerOperator(
  task_id= 'update_processed_low_watermark',
  image='amundsen_load',
  container_name='task__amundsen_update_processed_lowwatermark',
  api_version='auto',
  auto_remove=True,
  docker_url='unix://var/run/docker.sock',
  entrypoint=["/scripts/modules/load_env_run_script.sh", "/scripts/modules/calculate_delta_wm_metadata.py --agg_func min --watermark_type low_watermark --database default --schema processed"],
  #command=["min", "low_watermark"],
  network_mode="ml_platform",
  queue='queue_1',
  dag=dag
)

load_clean_table_stats = DockerOperator(
  task_id= 'profile_clean_tables',
  image='amundsen_load',
  container_name='task__amundsen_profile_clean_tables',
  api_version='auto',
  auto_remove=True,
  docker_url='unix://var/run/docker.sock',
  entrypoint=["/scripts/modules/load_env_run_script.sh", "/scripts/modules/calculate_schema_column_profiles.py --schema clean"],
  #command=["min", "low_watermark"],
  network_mode="ml_platform",
  queue='queue_1',
  dag=dag
)

load_processed_table_stats = DockerOperator(
  task_id= 'profile_processed_tables',
  image='amundsen_load',
  container_name='task__amundsen_profile_processed_tables',
  api_version='auto',
  auto_remove=True,
  docker_url='unix://var/run/docker.sock',
  entrypoint=["/scripts/modules/load_env_run_script.sh", "/scripts/modules/calculate_schema_column_profiles.py --schema processed"],
  #command=["min", "low_watermark"],
  network_mode="ml_platform",
  queue='queue_1',
  dag=dag
)

load_raw_table_stats = DockerOperator(
  task_id= 'profile_raw_tables',
  image='amundsen_load',
  container_name='task__amundsen_profile_raw_tables',
  api_version='auto',
  auto_remove=True,
  docker_url='unix://var/run/docker.sock',
  entrypoint=["/scripts/modules/load_env_run_script.sh", "/scripts/modules/calculate_schema_column_profiles.py --schema raw"],
  #command=["min", "low_watermark"],
  network_mode="ml_platform",
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
  entrypoint=["/scripts/modules/load_env_run_script.sh", "/scripts/modules/update_es_indices.py"],
  network_mode="ml_platform",
  queue='queue_1',
  dag=dag
)

clear_stale_es_data = DockerOperator(
  task_id= 'clear_stale_es_data',
  image='amundsen_load',
  container_name='task__amundsen_clear_stale_es',
  api_version='auto',
  auto_remove=True,
  docker_url='unix://var/run/docker.sock',
  entrypoint=["/scripts/modules/load_env_run_script.sh", "/scripts/modules/clear_stale_neo4j_data.py"],
  network_mode="ml_platform",
  queue='queue_1',
  dag=dag
)

end_dag = DummyOperator(
        task_id='end_dag',
        dag=dag
        )        

start_dag >> ingest_base_data


ingest_base_data >> update_high_watermark_raw >> load_raw_table_stats
ingest_base_data >> update_low_watermark_raw >> load_raw_table_stats

load_raw_table_stats >> update_elasticsearch


ingest_base_data >> update_high_watermark_clean >> load_clean_table_stats
ingest_base_data >> update_low_watermark_clean >> load_clean_table_stats 

load_clean_table_stats >> update_elasticsearch

ingest_base_data >> update_high_watermark_processed >> load_processed_table_stats
ingest_base_data >> update_low_watermark_processed >> load_processed_table_stats

load_processed_table_stats >> update_elasticsearch

update_elasticsearch >> clear_stale_es_data >> end_dag
