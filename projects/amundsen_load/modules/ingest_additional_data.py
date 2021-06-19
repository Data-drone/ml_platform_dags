### These scripts calculate stats needed for the populating Amundsen
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

#from utils import get_spark

SUPPORTED_HIVE_SCHEMAS = ['default']
SUPPORTED_HIVE_SCHEMA_SQL_IN_CLAUSE = "('{schemas}')".format(schemas="', '".join(SUPPORTED_HIVE_SCHEMAS))

LOGGER = logging.getLogger(__name__)

def connection_string():
    return 'hive://spark-thrift-server:10000/?auth=NOSASL'

def create_table_wm_job(**kwargs):
    sql = textwrap.dedent("""
        SELECT From_unixtime(A0.create_time) as create_time,
               'hive'                        as `database`,
               C0.NAME                       as `schema`,
               B0.tbl_name as table_name,
               {func}(A0.part_name) as part_name,
               {watermark} as part_type
        FROM   PARTITIONS A0
               LEFT OUTER JOIN TBLS B0
                            ON A0.tbl_id = B0.tbl_id
               LEFT OUTER JOIN DBS C0
                            ON B0.db_id = C0.db_id
        WHERE  C0.NAME IN {schemas}
               AND B0.tbl_type IN ( 'EXTERNAL_TABLE', 'MANAGED_TABLE' )
               AND A0.PART_NAME NOT LIKE '%%__HIVE_DEFAULT_PARTITION__%%'
        GROUP  BY C0.NAME, B0.tbl_name
        ORDER by create_time desc
    """).format(func=kwargs['templates_dict'].get('agg_func'),
                watermark=kwargs['templates_dict'].get('watermark_type'),
                schemas=SUPPORTED_HIVE_SCHEMA_SQL_IN_CLAUSE)

    LOGGER.info('SQL query: %s', sql)
    tmp_folder = '/var/tmp/amundsen/table_{hwm}'.format(hwm=kwargs['templates_dict'].get('watermark_type').strip("\""))
    node_files_folder = f'{tmp_folder}/nodes'
    relationship_files_folder = f'{tmp_folder}/relationships'

    hwm_extractor = SQLAlchemyExtractor()
    csv_loader = FsNeo4jCSVLoader()

    task = DefaultTask(extractor=hwm_extractor,
                       loader=csv_loader,
                       transformer=NoopTransformer())

    job_config = ConfigFactory.from_dict({
        f'extractor.sqlalchemy.{SQLAlchemyExtractor.CONN_STRING}': connection_string(),
        f'extractor.sqlalchemy.{SQLAlchemyExtractor.EXTRACT_SQL}': sql,
        'extractor.sqlalchemy.model_class': 'databuilder.models.watermark.Watermark',
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.NODE_DIR_PATH}': node_files_folder,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.RELATION_DIR_PATH}': relationship_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.NODE_FILES_DIR}': node_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.RELATION_FILES_DIR}': relationship_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_END_POINT_KEY}': kwargs['neo4j_endpoint'],
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_USER}': kwargs['neo4j_user'],
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_PASSWORD}': kwargs['neo4j_password'],
    })
    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=Neo4jCsvPublisher())
    job.launch()
    

if __name__ == '__main__':

    templates_dict={'agg_func': 'max',
                        'watermark_type': '"high_watermark"',
                        'part_regex': '{{ ds }}'}
    

    create_table_wm_job(
        neo4j_endpoint=os.environ['NEO4J_ENDPOINT'],
        neo4j_user = os.environ['NEO4J_USER'],
        neo4j_password = os.environ['NEO4J_PASSWORD'],
        templates_dict = templates_dict
    )

