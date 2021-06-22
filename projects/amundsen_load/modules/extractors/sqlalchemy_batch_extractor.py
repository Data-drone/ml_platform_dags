import importlib
from typing import Any

from pyhocon import ConfigFactory, ConfigTree
from sqlalchemy import create_engine

from multiprocessing.dummy import Pool as ThreadPool
from databuilder import Scoped
from databuilder.extractor.base_extractor import Extractor
from sqlalchemy.orm import sessionmaker
import pandas as pd

import logging

LOGGER = logging.getLogger("mainModule.extractor")

class SQLAlchemyBatchExtractor(Extractor):

    # Config keys
    CONN_STRING = 'conn_string'
    EXTRACT_TBL_LIST_QRY = 'SHOW TABLES' # SHOW TABLES IN xxxx
    TIMESTAMP_EXTRACTOR = 'pickup_datetime' # to run select max/min from
    CONNECT_ARGS = 'connect_args'
    DATABASE = 'default'
    SCHEMA = 'default'
    PART_TYPE = 'high_watermark'
    CLUSTER = 'my_delta_environment'

    DEFAULT_CONFIG = ConfigFactory.from_dict(
        {EXTRACT_TBL_LIST_QRY: "SHOW TABLES"}
    )

    def init(self, conf: ConfigTree) -> None:

        self.conf = conf.with_fallback(SQLAlchemyBatchExtractor.DEFAULT_CONFIG)
        self.conn_string = conf.get_string(SQLAlchemyBatchExtractor.CONN_STRING)

        self.extract_tbl_list = conf.get_string(SQLAlchemyBatchExtractor.EXTRACT_TBL_LIST_QRY) 
        self.timestamp_extractor = conf.get_string(SQLAlchemyBatchExtractor.TIMESTAMP_EXTRACTOR)

        self.database = conf.get_string(SQLAlchemyBatchExtractor.DATABASE)
        self.schema = conf.get_string(SQLAlchemyBatchExtractor.SCHEMA)
        self.part_type = conf.get_string(SQLAlchemyBatchExtractor.PART_TYPE)
        self.cluster  = conf.get_string(SQLAlchemyBatchExtractor.CLUSTER)

        # Not quite sure what model_class is
        model_class = conf.get('model_class', None)
        if model_class:
            module_name, class_name = model_class.rsplit(".", 1)
            mod = importlib.import_module(module_name)
            self.model_class = getattr(mod, class_name)

        LOGGER.info("starting processing")
        self.connection = self._get_connection()
        self.query_list = self._generate_query_list()
        self._execute_query()

    
    def close(self) -> None:
        if self.connection is not None:
            self.connection.close_all()

    def _get_connection(self) -> Any:
        """
        Creates a SQLAlchemy connection to Database and runs the query 
        in order to get the table list that we need
        """
        connect_args = {
            k: v
            for k, v in self.conf.get_config(
                self.CONNECT_ARGS, default=ConfigTree()
            ).items()
        }
        engine = create_engine(self.conn_string, connect_args=connect_args)
        #conn = engine.connect()
        session_factory = sessionmaker(bind=engine)
        LOGGER.info("sessionmaker started")

        return session_factory

    def _run_single_query(self, query_text):
        # all calls to Session() will create a thread-local session.
        # If we call upon the Session registry a second time, we get back the same Session.

        session = self.connection()
        query_result = session.execute(query_text[1]).fetchall()

        results = (query_text[0],query_result)
        
        return results

    def _thread_worker(self, query_tuple):
        query_results = self._run_single_query(query_tuple)
        return query_results

    def _work_parallel(self, query_list, thread_number: int=4):

        pool = ThreadPool(thread_number)
        results = pool.map(self._thread_worker, query_list)
        # If you don't care about the results, just comment the following 3 lines.
        pool.close()
        pool.join()

        LOGGER.info("results from workers are of type: {0}".format(type(results)))

        return results

    def _generate_query_list(self):

        """
        Runs the EXTRACT_TBL_LIST query so that we know what we need to run our max/min
        functions against
        """
        
        table_list = self.connection().execute(self.extract_tbl_list)
        table_df = pd.DataFrame(table_list, columns = ['database', 'tableName', 'isTemporary'])
        
        query_list = []
        for table in table_df.itertuples():
            sql = """select {0} from {1}.{2}""".format(self.timestamp_extractor, table[1], table[2])
            LOGGER.info("statement is {sql}".format(sql=sql))
            #sql = """select max({0}) from {1}.{2}""".format('pickup_datetime', table[1], table[2])
            query_list.append((table[2], sql))
        
        return query_list


    def _execute_query(self) -> None:

        """
        execute the queries to extract individual table metadata
        """

        max_test = self._work_parallel(self.query_list, 8)
        # the results get returned as list(tuple(string,list(tuple(str,null))))
        results_processed = [(x, y[0][0]) for (x,y) in max_test]

        ### reformat to what we need
        #### create_time, database

        results_processed_2 = [ {'create_time': y, 
                                    'database': self.database, 
                                    'schema': self.schema, 'table_name': x, 
                                    'part_name': 'ds='+str(y), 
                                    'part_type': self.part_type,
                                    'cluster': self.cluster} \
                                for (x,y) in results_processed ]
        

        self.iter = iter(results_processed_2)

    def extract(self) -> None:
        """
        Yield the sql result one at a time.
        convert the result to model if a model_class is provided
        """
        try:
            return next(self.iter)
        except StopIteration:
            return None
        except Exception as e:
            raise e

    def get_scope(self) -> str:
        return 'extractor.sqlalchemybatch'