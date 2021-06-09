import os
from pyspark.sql import SparkSession

packages = "io.delta:delta-core_2.12:1.0.0,org.apache.hadoop:hadoop-aws:3.2.0"
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages {0} pyspark-shell".format(packages)

#.config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider") \
    
spark = SparkSession \
    .builder \
    .appName("Load Data") \
    .config("spark.master", "spark://spark-master:7077") \
    .config("spark.num.executors", "2") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.access.key", os.environ['MINIO_ACCESS_KEY']) \
    .config("spark.hadoop.fs.s3a.secret.key", os.environ['MINIO_SECRET_KEY']) \
    .config("spark.hadoop.fs.s3a.endpoint", "172.30.0.4:9000") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .enableHiveSupport() \
    .getOrCreate()

green_trip_data_2015_test = "s3a://storage/raw_data/green_tripdata_2015-01.csv"
green_trip_2015_test = spark.read.option("header", True).csv(green_trip_data_2015_test)

green_trip_2015_test = green_trip_2015_test.withColumnRenamed("Trip_type ", "trip_type")

green_trip_2015_test.write.format("delta").mode('overwrite').save("s3a://storage/warehouse/green_taxi_2015_test")