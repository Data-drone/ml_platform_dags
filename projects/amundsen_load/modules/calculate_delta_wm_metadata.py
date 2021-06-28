import os
from pyhocon import ConfigFactory
import logging
import textwrap
import argparse

from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask
from extractors.sqlalchemy_batch_extractor import SQLAlchemyBatchExtractor
from databuilder.transformer.dict_to_model import MODEL_CLASS, DictToModel
import sys

LOGGER = logging.getLogger("mainModule")
LOGGER.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
LOGGER.addHandler(handler)

def connection_string():
    return 'hive://root@spark-thrift-server:10000/'

def create_table_wm_job(**kwargs):

    #sql = "SHOW TABLES"
    sql = kwargs['templates_dict'].get('extract_table_list')

    extractor_sql = "{func}(pickup_datetime)".format(func=kwargs['templates_dict'].get('agg_func'))

    LOGGER.info('Show Tables SQL query: %s', sql)
    LOGGER.info('Extractor SQL query: %s', extractor_sql)
    
    tmp_folder = '/var/tmp/amundsen/table_{hwm}'.format(hwm=kwargs['templates_dict'].get('watermark_type').strip("\""))
    node_files_folder = f'{tmp_folder}/nodes'
    relationship_files_folder = f'{tmp_folder}/relationships'

    hwm_extractor = SQLAlchemyBatchExtractor()
    csv_loader = FsNeo4jCSVLoader()

    task = DefaultTask(extractor=hwm_extractor,
                       loader=csv_loader,
                       transformer=DictToModel())

    LOGGER.info('Database setting prior to dict: %s', kwargs['templates_dict'].get('database'))

    job_config = ConfigFactory.from_dict({
        f'extractor.sqlalchemybatch.{SQLAlchemyBatchExtractor.CONN_STRING}': connection_string(),
        f'extractor.sqlalchemybatch.{SQLAlchemyBatchExtractor.EXTRACT_TBL_LIST_QRY}': sql,
        f'extractor.sqlalchemybatch.{SQLAlchemyBatchExtractor.TIMESTAMP_EXTRACTOR}': extractor_sql,
        f'extractor.sqlalchemybatch.{SQLAlchemyBatchExtractor.DATABASE}': kwargs['templates_dict'].get('database'),
        f'extractor.sqlalchemybatch.{SQLAlchemyBatchExtractor.SCHEMA}': kwargs['templates_dict'].get('schema'),
        f'extractor.sqlalchemybatch.{SQLAlchemyBatchExtractor.PART_TYPE}': kwargs['templates_dict'].get('watermark_type'),
        f'extractor.sqlalchemybatch.{SQLAlchemyBatchExtractor.CLUSTER}': kwargs['templates_dict'].get('cluster'),        
        f'extractor.sqlalchemybatch.{SQLAlchemyBatchExtractor.CONNECT_ARGS}': {'auth': 'NOSASL'},
        f'transformer.dict_to_model.{MODEL_CLASS}': 'databuilder.models.watermark.Watermark',
        #'extractor.sqlalchemy.model_class': 'databuilder.models.watermark.Watermark',
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.NODE_DIR_PATH}': node_files_folder,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.RELATION_DIR_PATH}': relationship_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.NODE_FILES_DIR}': node_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.RELATION_FILES_DIR}': relationship_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_END_POINT_KEY}': kwargs['neo4j_endpoint'],
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_USER}': kwargs['neo4j_user'],
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_PASSWORD}': kwargs['neo4j_password'],
        f'publisher.neo4j.{neo4j_csv_publisher.JOB_PUBLISH_TAG}': 'wm_test',
    })

    LOGGER.info("Job Config Database {0}".format(job_config.get('extractor.sqlalchemybatch')))

    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=Neo4jCsvPublisher())
    job.launch()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Extract High Watermark from Delta Tables')
    parser.add_argument('--agg_func', type=str, default='max', 
                        help='aggregation function for datetime col (min/max)')

    parser.add_argument('--watermark_type', type=str, default='high_watermark',
                        choices=['high_watermark', 'low_watermark'],
                        help='choose field where date gets recorded')

    ### Need to finish off adding args for template dict sections
    ## values other than default for delta and spark lake doesn't matter as we don't have a
    # cluster -> database -> schema level structure just cluster -> schema
    parser.add_argument('--database', type=str, default='default')
    
    # change this to move between raw / processed / clean
    parser.add_argument('--schema', type=str, default='default')

    args = parser.parse_args()

    LOGGER.info("agg_func: {0}, watermark: {1}".format(args.agg_func, args.watermark_type))
    LOGGER.info("database: {0}, schema: {1}".format(args.database, args.schema))

    templates_dict={'agg_func': args.agg_func,
                    'watermark_type': '{mark}'.format(mark=args.watermark_type),
                    'database': args.database,
                    'schema': args.schema,
                    'cluster': 'my_delta_environment',
                    'extract_table_list': "SHOW TABLES IN " + args.schema}


    create_table_wm_job(
        neo4j_endpoint=os.environ['NEO4J_ENDPOINT'],
        neo4j_user = os.environ['NEO4J_USER'],
        neo4j_password = os.environ['NEO4J_PASSWORD'],
        templates_dict = templates_dict
    )
