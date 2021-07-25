# the spark_setup file gets loaded in from py-files in spark submit
from spark_setup import get_spark

spark = get_spark()
spark = spark \
    .appName("Merge Green Schemas") \
    .enableHiveSupport() \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism*4)

green_trip_2015_table = spark.sql("select * from raw.green_taxi_pre2015")

green_trip_2015_table.write.format("delta").mode("overwrite").saveAsTable("raw.green_merged")

green_2015_h1 = spark.sql("select * from raw.green_taxi_2015_h1")

green_2015_h1.write.format("delta").mode("append") \
    .option("mergeSchema", "true").saveAsTable("raw.green_merged")

green_2015_h2_2016_h1 = spark.sql("select * from raw.green_taxi_2015_h2_2016_h1")

green_2015_h2_2016_h1.write.format("delta").mode("append") \
    .option("mergeSchema", "true").saveAsTable("raw.green_merged")