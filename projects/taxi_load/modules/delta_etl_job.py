# the spark_setup file gets loaded in from py-files in spark submit
from spark_setup import get_spark

spark = get_spark()
spark = spark \
    .appName("Merge Raw") \
    .enableHiveSupport() \
    .getOrCreate()

green_trip_2015_table = spark.sql("select * from green_taxi_pre2015")

green_trip_2015_table.write.format("delta").mode("overwrite").saveAsTable("green_merged")

green_2015_h1 = spark.sql("select * from green_taxi_2015_h1")

green_2015_h1.write.format("delta").mode("append") \
    .option("mergeSchema", "true").saveAsTable("green_merged")
