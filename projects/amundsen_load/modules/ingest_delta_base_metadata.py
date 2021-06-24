# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

"""
This script pulls from my Deltalake endpoint into Neo4J

Corresponds to Step 1 in the process

"""

from pyhocon import ConfigFactory
from databuilder.extractor.delta_lake_metadata_extractor import DeltaLakeMetadataExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.models.table_metadata import DESCRIPTION_NODE_LABEL
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask
from utils import get_spark

import os
import argparse

## To see the loggin data before
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def create_delta_lake_job_config(neo4j_endpoint, neo4j_user, neo4j_password,
                                cluster_key = 'my_delta_environment', database = 'default', 
                                schema_list = [], exclude_list = []    ):

    tmp_folder = '/var/tmp/amundsen/table_metadata'
    node_files_folder = f'{tmp_folder}/nodes/'
    relationship_files_folder = f'{tmp_folder}/relationships/'

    job_config = ConfigFactory.from_dict({
        f'extractor.delta_lake_table_metadata.{DeltaLakeMetadataExtractor.CLUSTER_KEY}': cluster_key,
        f'extractor.delta_lake_table_metadata.{DeltaLakeMetadataExtractor.DATABASE_KEY}': database,
        f'extractor.delta_lake_table_metadata.{DeltaLakeMetadataExtractor.SCHEMA_LIST_KEY}': schema_list,
        f'extractor.delta_lake_table_metadata.{DeltaLakeMetadataExtractor.EXCLUDE_LIST_SCHEMAS_KEY}': exclude_list,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.NODE_DIR_PATH}': node_files_folder,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.RELATION_DIR_PATH}': relationship_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.NODE_FILES_DIR}': node_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.RELATION_FILES_DIR}': relationship_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_END_POINT_KEY}': neo4j_endpoint,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_USER}': neo4j_user,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_PASSWORD}': neo4j_password,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_CREATE_ONLY_NODES}': [DESCRIPTION_NODE_LABEL],
        f'publisher.neo4j.job_publish_tag': 'some_unique_tag'  # TO-DO unique tag must be added
    })
    return job_config


def ingest_deltalake(neo4j_endpoint: str, neo4j_user: str, neo4j_password: str,
                    cluster_key: str, database: str, 
                        schema_list: list, exclude_list: list):
    """
    Ingests metadata from our delta lake

    """

    packages = """io.delta:delta-core_2.12:1.0.0,org.apache.hadoop:hadoop-aws:3.2.0"""
    os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages {0} pyspark-shell".format(packages)

    spark = get_spark()
    spark = spark \
            .appName("Amundsen Delta Lake Metadata Extraction") \
            .config("spark.driver.cores", "4") \
            .config("spark.executor.cores", "4") \
            .config("spark.num.executors", "2") \
            .config("spark.driver.memory", "6g") \
            .config("spark.executor.memory", "6g") \
            .enableHiveSupport() \
            .getOrCreate()

    job_config = create_delta_lake_job_config(neo4j_endpoint, neo4j_user, neo4j_password,
                                    cluster_key, database, schema_list, exclude_list)
    dExtractor = DeltaLakeMetadataExtractor()
    dExtractor.set_spark(spark)
    job = DefaultJob(conf=job_config,
                        task=DefaultTask(extractor=dExtractor, loader=FsNeo4jCSVLoader()),
                        publisher=Neo4jCsvPublisher())
    job.launch()

    spark.stop()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Extract DeltaLake Metadata')
    parser.add_argument('--cluster_key', metavar='cluster_key', type=str, default='my_delta_environment',
                        help="The name of the cluster that we are parsing")
    
    parser.add_argument('--database', metavar='database', type=str, default='default')

    ## these require use of python 3.8+
    parser.add_argument('--schema', metavar='schema', action="append", 
                        help='Schemas that we will be parsing to find tables eg (raw/clean/processed)')
    
    parser.add_argument('--exclude_list', metavar='exclude_list', action="append")

    args = parser.parse_args()

    neo_endpoint = os.environ['NEO4J_ENDPOINT']
    neo_user = os.environ['NEO4J_USER']
    neo_password = os.environ['NEO4J_PASSWORD']

    ingest_deltalake(neo_endpoint, neo_user, neo_password,
                        cluster_key = args.cluster_key, database = args.database, 
                        schema_list = args.schema, exclude_list = args.exclude_list)