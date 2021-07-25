# the spark_setup file gets loaded in from py-files in spark submit
from spark_setup import get_spark

spark = get_spark()
spark = spark \
    .appName("Merge various taxi datasources") \
    .enableHiveSupport() \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism*4)

clean_green = spark.sql("select * from clean.green_clean")
clean_yellow = spark.sql("select * from clean.yellow_clean")

merged_taxi_table = clean_green.unionByName(clean_yellow, allowMissingColumns=True)

spark.sql("CREATE DATABASE IF NOT EXISTS processed LOCATION 's3a://storage/warehouse/processed'")

merged_taxi_table.write.partitionBy('pickup_year', 'pickup_month').format("delta").mode("overwrite") \
    .saveAsTable("processed.nyc_taxi_dataset")
