import os
from pyspark.sql import SparkSession
import sys

packages = "io.delta:delta-core_2.12:1.0.0,org.apache.hadoop:hadoop-aws:3.2.0"
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages {0} pyspark-shell".format(packages)

#.config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider") \
    
spark = SparkSession \
    .builder \
    .appName("Jupyter") \
    .config("spark.master", "spark://spark-master:7077") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.access.key", os.environ['MINIO_ACCESS_KEY']) \
    .config("spark.hadoop.fs.s3a.secret.key", os.environ['MINIO_SECRET_KEY']) \
    .config("spark.hadoop.fs.s3a.endpoint", "minio:9000") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.metastore.catalog.default", "hive") \
    .config("spark.sql.warehouse.dir", "s3a://storage/warehouse") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.maximum", "50") \
    .enableHiveSupport() \
    .getOrCreate()

def process_table(read_path, table_name):
    green_trip_data_pre2015_path = "s3a://storage/raw_data/green_tripdata_201[3-4]*.csv"
    green_trip_data_pre2015 = spark.read.option("header", True).csv(read_path)

    green_trip_data_pre2015 = green_trip_data_pre2015.withColumnRenamed("Trip_type ", "trip_type")

    green_trip_data_pre2015.write.format("delta").mode("overwrite").saveAsTable(table_name)

# input args from airflow
read_path = sys.argv[1].split('/')
output_table = sys.argv[2]

read_path = ['s3a://storage/raw_data/' + file for file in read_path]
print(read_path)

process_table(read_path, output_table)