from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os, traceback, time
from pyspark.sql.streaming import StreamingQueryException
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.types import _parse_datatype_json_string
import logging

# Set the location of the Delta Lake and Kafka packages
delta_package = "io.delta:delta-spark_2.12:3.0.0"  # Replace with the correct Delta version
kafka_package = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"  # Replace with the correct Spark version
xml_package = "com.databricks:spark-xml_2.12:0.14.0"

kafka_server = "spark-test1:9092"
topic_name = "test-topic"
hdfs_path = "hdfs://spark-test1:9000"
raw = os.path.join(hdfs_path, 'raw', 'transactions')
checkpoint = os.path.join(hdfs_path, 'checkpoint', 'raw', 'transactions')
dlq = os.path.join(hdfs_path, 'dlq', 'raw', 'transactions')

# Initialize Spark Session with Delta Lake and Kafka support
spark = SparkSession.builder \
    .appName("write_to_raw") \
    .master('spark://spark-test1:7077') \
    .config("spark.jars.packages", f"{delta_package},{kafka_package},{xml_package}") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.cores.max", "1") \
    .config("spark.executor.memory", "512m") \
    .getOrCreate()

def ext_from_xml(xml_column, schema, options={}):
    java_column = _to_java_column(xml_column.cast('string'))
    java_schema = spark._jsparkSession.parseDataType(schema.json())
    scala_map = spark._jvm.org.apache.spark.api.python.PythonUtils.toScalaMap(options)
    jc = spark._jvm.com.databricks.spark.xml.functions.from_xml(
        java_column, java_schema, scala_map)
    return Column(jc)

def ext_schema_of_xml_df(df, options={}):
    assert len(df.columns) == 1

    scala_options = spark._jvm.PythonUtils.toScalaMap(options)
    java_xml_module = getattr(getattr(
        spark._jvm.com.databricks.spark.xml, "package$"), "MODULE$")
    java_schema = java_xml_module.schema_of_xml_df(df._jdf, scala_options)
    return _parse_datatype_json_string(java_schema.json())

# Function to process each batch
def process_batch(batch_df, batch_id):
    if not batch_df.rdd.isEmpty():
        #batch_df.show()  # or any other processing you want to do
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
    # Read from Kafka
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
        # Log the error message
        print(f"Streaming exception:\n{traceback.format_exc()}")
        print("Restarting query after 10 seconds...")
        time.sleep(10)  # Sleep for 10 seconds before restarting the query
    except Exception as e:
        print(f"Non-streaming exception:\n{traceback.format_exc()}")
        print(f"Restarting query after 10 seconds...")        
        time.sleep(10)