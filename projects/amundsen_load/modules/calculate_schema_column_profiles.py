from calculate_delta_column_profiles import create_pandas_profiling_job_config, profile_deltalake_tables

import argparse
import os
from sqlalchemy import create_engine 
from sqlalchemy.orm import sessionmaker
import pandas as pd

def connection_engine(con_args):
    
    """
    We have hardcoded the connect args for now
    """

    connection = create_engine('hive://root@spark-thrift-server:10000/', connect_args=con_args)

    return connection


def get_table_list(schema_to_parse, con_args):

    """
    Read the table list for a particular schema from the 
    """

    sql = "SHOW TABLES IN {schema}".format(schema=schema_to_parse)

    connection = connection_engine(con_args)

    table_result = connection.execute(sql)

    table_df = pd.DataFrame(table_result, columns = ['database', 'tableName', 'isTemporary'])

    table_list = table_df['tableName'].tolist()

    return table_list 



if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Profile a table')


    # For our deltalake structure, we don't need database schema is what matters
    parser.add_argument('--database', metavar='database', type=str, default='default')
    
    # we should adjust this to run on a per schema basis?
    parser.add_argument('--schema', metavar='schema', type=str, default='default')
    
    
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

    table_list = get_table_list(args.schema, connect_args)

    for table in table_list:
        profile_deltalake_tables(neo_endpoint, neo_user, neo_password,
                            args.database, table, args.schema, 
                            args.cluster, connect_args)

    