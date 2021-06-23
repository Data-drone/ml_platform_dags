# the spark_setup file gets loaded in from py-files in spark submit
from spark_setup import get_spark
import pyspark.sql.functions as F

spark = get_spark()
spark = spark \
    .appName("Clean Yellow Table") \
    .enableHiveSupport() \
    .getOrCreate()


yellow_merged = spark.sql("select * from raw.yellow_merged")

yellow_processed = yellow_merged \
    .withColumn('pickup_datetime', F.to_timestamp('pickup_datetime')) \
    .withColumn('dropoff_datetime', F.to_timestamp('dropoff_datetime')) \
    .withColumn('passenger_count', F.col('passenger_count').cast('integer')) \
    .withColumn('trip_distance', F.col('trip_distance').cast('float')) \
    .withColumn('pickup_longitude', F.col('pickup_longitude').cast('float')) \
    .withColumn('pickup_latitude', F.col('pickup_latitude').cast('float')) \
    .withColumn('rate_code_id', F.col('rate_code')) \
    .withColumn('dropoff_longitude', F.col('dropoff_longitude').cast('float')) \
    .withColumn('dropoff_latitude', F.col('dropoff_latitude').cast('float')) \
    .withColumn('payment_type', F.col('payment_type').cast('integer')) \
    .withColumn('fare_amount', F.col('fare_amount').cast('float')) \
    .withColumn('improvement_surcharge', F.col('surcharge').cast('float')) \
    .withColumn('mta_tax', F.col('mta_tax').cast('float')) \
    .withColumn('tip_amount', F.col('tip_amount').cast('float')) \
    .withColumn('tolls_amount', F.col('tolls_amount').cast('float')) \
    .withColumn('total_amount', F.col('total_amount').cast('float')) \
    .withColumn('pickup_year', F.year('pickup_datetime')) \
    .withColumn('pickup_month', F.month('pickup_datetime')) \
    .drop('surcharge')

spark.sql("CREATE DATABASE IF NOT EXISTS clean LOCATION 's3a://storage/warehouse/clean'")

yellow_processed.write.partitionBy('pickup_year', 'pickup_month').format("delta").mode("overwrite").saveAsTable("clean.yellow_clean")
