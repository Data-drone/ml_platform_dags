# the spark_setup file gets loaded in from py-files in spark submit
from spark_setup import get_spark
import pyspark.sql.functions as F

spark = get_spark()
spark = spark \
    .appName("Clean Green") \
    .enableHiveSupport() \
    .getOrCreate()

green_merged = spark.sql("select * from green_merged")

green_processed = green_merged \
    .withColumn('pickup_datetime', F.to_timestamp('lpep_pickup_datetime')) \
    .withColumn('dropoff_datetime', F.to_timestamp('Lpep_dropoff_datetime')) \
    .withColumn('rate_code_id', F.col('RateCodeID').cast('integer')) \
    .withColumn('pickup_longitude', F.col('Pickup_longitude').cast('float')) \
    .withColumn('pickup_latitude', F.col('Pickup_latitude').cast('float')) \
    .withColumn('dropoff_longitude', F.col('Dropoff_longitude').cast('float')) \
    .withColumn('dropoff_latitude', F.col('Dropoff_latitude').cast('float')) \
    .withColumn('passenger_count', F.col('Passenger_count').cast('integer')) \
    .withColumn('trip_distance', F.col('Trip_distance').cast('float')) \
    .withColumn('fare_amount', F.col('Fare_amount').cast('float')) \
    .withColumn('extra', F.col('Extra').cast('float')) \
    .withColumn('mta_tax', F.col('MTA_tax').cast('float')) \
    .withColumn('tip_amount', F.col('Tip_amount').cast('float')) \
    .withColumn('tolls_amount', F.col('Tolls_amount').cast('float')) \
    .withColumn('ehail_fee', F.col('Ehail_fee').cast('float')) \
    .withColumn('total_amount', F.col('Total_amount').cast('float')) \
    .withColumn('payment_type', F.col('Payment_type').cast('integer')) \
    .withColumn('trip_type', F.col('trip_type').cast('integer')) \
    .withColumn('improvement_surcharge', F.col('improvement_surcharge').cast('float')) \
    .select(
        F.col('VendorID').alias('vendor_id'), 'pickup_datetime', 'dropoff_datetime',
        F.col('Store_and_fwd_flag').alias('store_and_fwd_flag'), 'rate_code_id',
        'pickup_longitude', 'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude',
        'passenger_count', 'trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount',
        'tolls_amount', 'ehail_fee', 'total_amount', 'payment_type', 'trip_type', 'improvement_surcharge'
    )

green_processed.write.format("delta").mode("overwrite").saveAsTable("green_clean")
