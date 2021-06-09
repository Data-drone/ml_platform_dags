## Normal Ingest as we need different s3 configs and cannot do that within a spark session
import boto3
from botocore import UNSIGNED
from botocore.client import Config
from minio import Minio
import os

def load_data_to_minio(file_to_load: str, file_save_name: str, write_path_minio: str):
    ## Test paths
    # file_to_load: 'trip data/green_tripdata_2015-01.csv'
    # file_save_name: 'green_tripdata_2015-01.csv'
    # write_path_minio: 'raw_data/green_tripdata_2015-01.csv'

    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    #bucket = s3.Bucket('nyc-tlc')
    with open(file_save_name, 'wb') as f:
        s3.download_fileobj('nyc-tlc', file_to_load, f)

    client = Minio(
        "minio:9000",
        access_key=os.environ['MINIO_ACCESS_KEY'],
        secret_key=os.environ['MINIO_SECRET_KEY'],
        secure=False
    )

    client.fput_object('storage', write_path_minio, 
                   file_save_name)

    

        


