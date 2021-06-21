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

LOGGER = logging.getLogger(__name__)

class SQLAlchemyBatchExtractor(Extractor):

    # Config keys
    CONN_STRING = 'conn_string'
    EXTRACT_TBL_LIST_QRY = 'SHOW TABLES' # SHOW TABLES IN xxxx
    TIMESTAMP_COL = 'pickup_datetime' # to run select max/min from
    CONNECT_ARGS = 'connect_args'

    DEFAULT_CONFIG = ConfigFactory.from_dict(
        {EXTRACT_TBL_LIST_QRY: "SHOW TABLES"}
    )

    def init(self, conf: ConfigTree) -> None:

        self.conf = conf.with_fallback(SQLAlchemyBatchExtractor.DEFAULT_CONFIG)
        self.conn_string = conf.get_string(SQLAlchemyBatchExtractor.CONN_STRING)

        self.connection = self._get_connection()

        self.extract_tbl_list = conf.get_string(SQLAlchemyBatchExtractor.EXTRACT_TBL_LIST_QRY) 
        self.timestamp_col = conf.get_string(SQLAlchemyBatchExtractor.TIMESTAMP_COL)

        # Not quite sure what model_class is
        model_class = conf.get('model_class', None)
        if model_class:
            module_name, class_name = model_class.rsplit(".", 1)
            mod = importlib.import_module(module_name)
            self.model_class = getattr(mod, class_name)

        self.query_list = self._generate_query_list()
        self._execute_query()

    
    def close(self) -> None:
        pass

    def _get_connection(self) -> Any:
        """
        Create a SQLAlchemy connection to Database
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

        # dunno why this isn't working.. self.extract_tbl_list
        tbl_list = session_factory().execute("SHOW TABLES").fetchall()
        self.tbl_df = pd.DataFrame(tbl_list, 
                        columns = ['database', 'tableName', 'isTemporary'])

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
        
        table_list = self.connection().execute(self.extract_tbl_list)
        table_df = pd.DataFrame(table_list, columns = ['database', 'tableName', 'isTemporary'])
        
        query_list = []
        for table in table_df.itertuples():
            sql = """select max({0}) from {1}.{2}""".format('pickup_datetime', table[1], table[2])
            query_list.append((table, sql))
        
        #self.query_list = query_list
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
        results_processed_2 = [ (y, 'default', 'default', x, y, 'high_watermark') \
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