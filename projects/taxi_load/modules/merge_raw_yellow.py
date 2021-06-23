# the spark_setup file gets loaded in from py-files in spark submit
from spark_setup import get_spark

spark = get_spark()
spark = spark \
    .appName("Merge Yellow Schemas") \
    .enableHiveSupport() \
    .getOrCreate()

yellow_pre_2015_tables = spark.sql("select * from raw.yellow_taxi_pre2015")

yellow_pre_2015_tables.write.format("delta").mode("overwrite") \
    .saveAsTable("raw.yellow_merged")
