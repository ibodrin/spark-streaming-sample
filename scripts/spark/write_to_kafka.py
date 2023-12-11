import random, time, threading, os, glob
from random import randint
import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import *
from pyspark.sql.streaming import StreamingQueryException
import traceback, logging

host = 'spark-test1'
checkpoint = 'hdfs://spark-test1:9000/checkpoint/raw/transactions'
xml_directory = 'hdfs://spark-test1:9000/in/transactions'

kafka_package = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

spark = SparkSession \
    .builder \
    .appName("write_to_kafka") \
    .master(f"spark://{host}:7077") \
    .config("spark.jars.packages", f"{kafka_package}") \
    .config("spark.sql.streaming.checkpointLocation", checkpoint) \
    .config("spark.cores.max", "1") \
    .config("spark.executor.memory", "512m") \
    .getOrCreate()

kafka_server = f"{host}:9092"
topic_name = "test-topic"
logger = logging.getLogger(__name__)

def stream(spark):
    schema = StructType([
        StructField("path", StringType(), False),
        StructField("modificationTime", TimestampType(), False),
        StructField("length", LongType(), False),
        StructField("content", BinaryType(), True)
    ])

    # Read stream from directory
    df = spark.readStream.format("binaryFile") \
        .schema(schema) \
        .load(xml_directory)

    # Write the stream to Kafka
    return df.selectExpr("path as key", "to_json(struct(*)) AS value").writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("topic", topic_name) \
        .start()

    # return df \
    #     .writeStream \
    #     .outputMode("append") \
    #     .format("console") \
    #     .start() \
    #     .awaitTermination()


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