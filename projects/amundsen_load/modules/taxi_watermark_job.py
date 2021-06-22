import os
from pyhocon import ConfigFactory
import logging
import textwrap

from databuilder.extractor.hive_table_metadata_extractor import HiveTableMetadataExtractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.models.table_metadata import DESCRIPTION_NODE_LABEL
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import NoopTransformer
from extractors.sqlalchemy_batch_extractor import SQLAlchemyBatchExtractor
from databuilder.transformer.dict_to_model import MODEL_CLASS, DictToModel

LOGGER = logging.getLogger(__name__)
logging.getLogger().addHandler(logging.StreamHandler())

def connection_string():
    return 'hive://root@spark-thrift-server:10000/'

def create_table_wm_job(**kwargs):

    sql = "SHOW TABLES"

    extractor_sql = "{0}(pickup_datetime)".format(kwargs['templates_dict'].get('agg_func'))

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
    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=Neo4jCsvPublisher())
    job.launch()


if __name__ == '__main__':

    templates_dict={'agg_func': 'max',
                    'watermark_type': '"high_watermark"',
                    'database': 'default',
                    'schema': 'default',
                    'cluster': 'my_delta_lake'}
    

    create_table_wm_job(
        neo4j_endpoint=os.environ['NEO4J_ENDPOINT'],
        neo4j_user = os.environ['NEO4J_USER'],
        neo4j_password = os.environ['NEO4J_PASSWORD'],
        templates_dict = templates_dict
    )
