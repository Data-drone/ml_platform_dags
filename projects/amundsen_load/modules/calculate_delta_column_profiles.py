
import pandas as pd
import pandas_profiling
from sqlalchemy import create_engine

from pyhocon import ConfigFactory
from databuilder.extractor.pandas_profiling_column_stats_extractor import PandasProfilingColumnStatsExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask

from utils import get_spark
import os

## command line args
import argparse


def create_pandas_profiling_job_config(neo4j_endpoint, neo4j_user, neo4j_password,
                                        database, table, schema, cluster, report_file):

    tmp_folder = '/var/tmp/amundsen/table_metadata'
    node_files_folder = f'{tmp_folder}/nodes/'
    relationship_files_folder = f'{tmp_folder}/relationships/'
    

    job_config = ConfigFactory.from_dict({
        f'extractor.pandas_profiling.{PandasProfilingColumnStatsExtractor.FILE_PATH}': report_file,
        f'extractor.pandas_profiling.{PandasProfilingColumnStatsExtractor.DATABASE_NAME}': database,
        f'extractor.pandas_profiling.{PandasProfilingColumnStatsExtractor.TABLE_NAME}': table,
        f'extractor.pandas_profiling.{PandasProfilingColumnStatsExtractor.SCHEMA_NAME}': schema,
        f'extractor.pandas_profiling.{PandasProfilingColumnStatsExtractor.CLUSTER_NAME}': cluster,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.NODE_DIR_PATH}': node_files_folder,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.RELATION_DIR_PATH}': relationship_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.NODE_FILES_DIR}': node_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.RELATION_FILES_DIR}': relationship_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_END_POINT_KEY}': neo4j_endpoint,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_USER}': neo4j_user,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_PASSWORD}': neo4j_password,
        f'publisher.neo4j.{neo4j_csv_publisher.JOB_PUBLISH_TAG}': 'wm_test',
    })
    return job_config


def profile_deltalake_tables(neo4j_endpoint: str, neo4j_user: str, neo4j_password: str,
                                database: str, table: str, schema: str, cluster: str, connect_args: dict):

    """

    Profiles all the tables

    """

    report_file = f'/tmp/table_report.json'

    job_config = create_pandas_profiling_job_config(neo4j_endpoint, neo4j_user, neo4j_password,
                                                        database, table, schema, cluster, report_file)
    spark = get_spark()
    # beefing driver for pandas profiling
    spark = spark \
            .appName("Amundsen Column Profiling") \
            .config("spark.driver.cores", "4") \
            .config("spark.executor.cores", "4") \
            .config("spark.num.executors", "2") \
            .config("spark.driver.memory", "24g") \
            .config("spark.executor.memory", "6g") \
            .enableHiveSupport() \
            .getOrCreate()

    df = spark.sql("select * from {schema}.{table} TABLESAMPLE (100000 ROWS)".format(schema=schema, table=table))
    
    df = df.toPandas()

    ## select numerics only at the moment the column profiler only supports numerics
    df = df.select_dtypes(include='number')

    report = df.profile_report(sort=None, minimal=True)
    report.to_file(report_file)

    ppExtractor = PandasProfilingColumnStatsExtractor()

    job = DefaultJob(conf=job_config,
                     task=DefaultTask(extractor=ppExtractor, loader=FsNeo4jCSVLoader()),
                    publisher=Neo4jCsvPublisher())

    job.launch()

    spark.stop()


if __name__=='__main__':

    parser = argparse.ArgumentParser(description='Profile a table')


    # For our deltalake structure, we don't need database schema is what matters
    parser.add_argument('--database', metavar='database', type=str, default='default')
    
    # we should adjust this to run on a per schema basis?
    parser.add_argument('--schema', metavar='schema', type=str, default='default')
    
    # speccing tables as well makes it a bit harder? 
    parser.add_argument('--table', metavar='table', type=str)
    parser.add_argument('--cluster', metavar='cluster', type=str, default='my_delta_environment')


    args = parser.parse_args()

    neo_endpoint = os.environ['NEO4J_ENDPOINT']
    neo_user = os.environ['NEO4J_USER']
    neo_password = os.environ['NEO4J_PASSWORD']

    connect_args = {'auth': 'NOSASL'}
    #database
    #table
    #schema
    #cluster

    ### default
    # python3 delta_pandas_profiling.py --database default --table green_merged --schema default --cluster my_delta_environment

    profile_deltalake_tables(neo_endpoint, neo_user, neo_password,
                            args.database, args.table, args.schema, 
                            args.cluster, connect_args)

    