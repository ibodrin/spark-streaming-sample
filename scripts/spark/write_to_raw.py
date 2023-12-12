from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os, traceback, time
from pyspark.sql.streaming import StreamingQueryException

kafka_package = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

kafka_server = "spark-test1:9092"
topic_name = "test-topic"
hdfs_path = "hdfs://spark-test1:9000"
raw = os.path.join(hdfs_path, 'raw', 'transactions')
checkpoint = os.path.join(hdfs_path, 'checkpoint', 'raw', 'transactions')
dlq = os.path.join(hdfs_path, 'dlq', 'raw', 'transactions')

spark = SparkSession.builder \
    .appName("write_to_raw") \
    .master('spark://spark-test1:7077') \
    .config("spark.jars.packages", f"{kafka_package}") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.cores.max", "1") \
    .config("spark.executor.memory", "512m") \
    .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

def process_batch(batch_df, batch_id):
    if not batch_df.rdd.isEmpty():
        try:
            files_count = batch_df.count()
            batch_df.select(
                '*',
                current_date().alias("_raw_insert_date"),
                date_format(current_timestamp(), "HH").alias("_raw_insert_hour"),
                current_timestamp().alias("_raw_insert_timestamp")
            ).write \
            .mode("append") \
            .partitionBy("_raw_insert_date", "_raw_insert_hour") \
            .parquet(raw)

        except Exception as e:
            error_text = traceback.format_exc()
            print(f"Exception occurred during batch processing:\n{error_text}")
            error_df = batch_df.selectExpr("CAST(value AS STRING) as xml_data") \
                            .withColumn("_raw_error_text", lit(error_text)) \
                            .withColumn("_raw_insert_date", current_date()) \
                            .withColumn("_raw_insert_hour", date_format(current_timestamp(), "HH")) \
                            .withColumn("_raw_insert_timestamp", current_timestamp())
            error_df.write.mode("append").partitionBy("_raw_insert_date", "_raw_insert_hour").format("parquet").save(dlq)
    else:
        print("Empty batch")

def stream(spark):
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("subscribe", topic_name) \
        .load() \
        .selectExpr(
            "CAST(key AS STRING)",
            "CAST(value AS STRING)",
            "CAST(topic AS STRING)",
            "CAST(partition AS STRING)",
            "CAST(offset AS STRING)",
            "CAST(timestamp AS STRING)",
            "CAST(timestampType AS STRING)" 
        )
    
    return df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", checkpoint) \
        .start()
    
    # return df \
    #     .writeStream \
    #     .outputMode("append") \
    #     .format("console") \
    #     .start()  

while True:
    try:
        stream(spark).awaitTermination()
    except StreamingQueryException as e:
        print(f"Streaming exception:\n{traceback.format_exc()}")
        print("Restarting query after 10 seconds...")
        time.sleep(10)
    except Exception as e:
        print(f"Non-streaming exception:\n{traceback.format_exc()}")
        print(f"Restarting query after 10 seconds...")        
        time.sleep(10)