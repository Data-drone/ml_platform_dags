# the spark_setup file gets loaded in from py-files in spark submit
from spark_setup import get_spark
import logging
import sys

# We need to set the logger to py4j to get it to come up on the spark loggers
logger = logging.getLogger('py4j')


spark = get_spark()    
spark = spark \
    .appName("Ingest to Raw Zone") \
    .enableHiveSupport() \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism*4)

def process_table(read_path: str, table_name: str):
    #green_trip_data_pre2015_path = "s3a://storage/raw_data/green_tripdata_201[3-4]*.csv"
    raw_data = spark.read.option("header", True).csv(read_path)

    logger.info("Read Data")

    # green tables have this quirk - do others?
    raw_fix = raw_data.withColumnRenamed("Trip_type ", "trip_type")
    raw_fix = raw_fix.withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
    raw_fix = raw_fix.withColumnRenamed("Lpep_dropoff_datetime", "dropoff_datetime")

    # fix yellow table quirks
    raw_fix = raw_fix \
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
    
    logger.info("Writing Back Data")

    spark.sql("CREATE DATABASE IF NOT EXISTS raw LOCATION 's3a://storage/warehouse/raw'")

    #.option("overwriteSchema", "true") is a setting for delta lake
    raw_fix.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable("raw."+table_name)

# input args from airflow
read_path = sys.argv[1].split('/')
output_table = sys.argv[2]

read_path = ['s3a://storage/raw_data/' + file for file in read_path]
print(read_path)

process_table(read_path, output_table)