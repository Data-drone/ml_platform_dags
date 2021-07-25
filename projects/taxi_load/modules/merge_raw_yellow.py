# the spark_setup file gets loaded in from py-files in spark submit
from spark_setup import get_spark

spark = get_spark()
spark = spark \
    .appName("Merge Yellow Schemas") \
    .enableHiveSupport() \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism*4)

yellow_pre_2015_tables = spark.sql("select * from raw.yellow_taxi_pre2015")

yellow_pre_2015_tables.write.format("delta").mode("overwrite") \
    .saveAsTable("raw.yellow_merged")

yellow_2015_2016_h1 = spark.sql("select * from raw.yellow_taxi_2015_2016_h1")

## some columns got renamed so we need to fix
yellow_2015_2016_h1 = yellow_2015_2016_h1 \
    .withColumnRenamed("VendorID", "vendor_id") \
    .withColumnRenamed("RatecodeID", "rate_code")
    
yellow_2015_2016_h1.write.format("delta").mode("append") \
    .option("mergeSchema", "true").saveAsTable("raw.yellow_merged")

