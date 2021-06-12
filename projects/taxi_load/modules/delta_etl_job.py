import os
from pyspark.sql import SparkSession

packages = "io.delta:delta-core_2.12:1.0.0,org.apache.hadoop:hadoop-aws:3.2.0"
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages {0} pyspark-shell".format(packages)

#.config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider") \
    
spark = SparkSession \
    .builder \
    .appName("Load Data") \
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

green_trip_2015_table = spark.sql("select * from green_taxi_pre2015")

green_trip_2015_table.write.format("delta").mode("overwrite").saveAsTable("green_merged")

green_2015_h1 = spark.sql("select * from green_taxi_2015_h1")

green_2015_h1.write.format("delta").mode("append") \
    .option("mergeSchema", "true").saveAsTable("green_merged")
