# the spark_setup file gets loaded in from py-files in spark submit
from spark_setup import get_spark

import sys

#.config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider") \

spark = get_spark()    
spark = spark \
    .appName("Ingest to Raw Lake") \
    .enableHiveSupport() \
    .getOrCreate()

def process_table(read_path: str, table_name: str):
    #green_trip_data_pre2015_path = "s3a://storage/raw_data/green_tripdata_201[3-4]*.csv"
    raw_data = spark.read.option("header", True).csv(read_path)

    # green tables have this quirk - do others?
    raw_fix = raw_data.withColumnRenamed("Trip_type ", "trip_type")

    raw_fix.write.format("delta").mode("overwrite").saveAsTable(table_name)

# input args from airflow
read_path = sys.argv[1].split('/')
output_table = sys.argv[2]

read_path = ['s3a://storage/raw_data/' + file for file in read_path]
print(read_path)

process_table(read_path, output_table)