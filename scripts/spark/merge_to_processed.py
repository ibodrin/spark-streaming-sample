from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.types import _parse_datatype_json_string
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.streaming import StreamingQueryException
import traceback, time, os, logging

delta_package = "io.delta:delta-spark_2.12:3.0.0"
xml_package = "com.databricks:spark-xml_2.12:0.14.0"
spark = SparkSession.builder.appName("merge_to_processed").master('spark://spark-test1:7077') \
    .config("spark.jars.packages", f"{delta_package},{xml_package}") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.cores.max", "1") \
    .config("spark.executor.memory", "512m") \
    .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

hdfs_path = "hdfs://spark-test1:9000"
raw = os.path.join(hdfs_path, 'raw', 'transactions')
processed = os.path.join(hdfs_path, 'processed', 'transactions')
checkpoint = os.path.join(hdfs_path, 'checkpoint', 'processed', 'transactions')
dlq = os.path.join(hdfs_path, 'dlq', 'processed', 'transactions')

# Mapping XML parsing functions as per https://github.com/databricks/spark-xml?tab=readme-ov-file#pyspark-notes
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

def process_batch(batch_df, batch_id):
    if not batch_df.rdd.isEmpty():
        files_count = batch_df.count()

        json_schema = StructType([
            StructField("path", StringType()),
            StructField("modificationTime", StringType()),
            StructField("length", StringType()),
            StructField("content", StringType())
        ])
        parsed_json_df = batch_df.withColumn("json_data", from_json(col("value"), json_schema))

        # Extract and decode the base64 content
        decoded_df = parsed_json_df.withColumn("decoded_content", unbase64(col("json_data.content"))) \
            .withColumn("xml_content", expr("CAST(decoded_content AS STRING)"))

        #schema_def = ext_schema_of_xml_df(decoded_df.select("xml_content"))
        #decoded_df = decoded_df.withColumn('test_debug', lit(schema_def).cast('string'))

        xml_schema = StructType([
            StructField(
                'Transaction', 
                ArrayType(
                    StructType([
                        StructField('TransactionId', LongType(), True),
                        StructField('Amount', FloatType(), True),
                        StructField('CustomerId', LongType(), True),
                        StructField('DateTime', TimestampType(), True),
                        StructField('Location', StringType(), True),
                        StructField('Result', StringType(), True)
                    ]),
                    True
                ),
                True
            )
        ])

        xml_df = decoded_df.withColumn(
            "parsed",
            ext_from_xml(
                xml_column = col("xml_content"),
                schema=xml_schema,
                options={"mode": "FAILFAST", "columnNameOfCorruptRecord": "_corrupt_file"}
            )
        )
        
        windowSpec = Window.partitionBy("TransactionId").orderBy(col("_raw_insert_timestamp").desc())
        # Flatten the DataFrame
        flattened_df = xml_df.select(
            explode(col("parsed.Transaction")).alias("Transaction"),
            col('_raw_insert_timestamp').alias('_raw_insert_timestamp')
        ).select(
            col("Transaction.TransactionId").alias("TransactionId"),
            col("Transaction.Amount").alias("Amount"),
            col("Transaction.CustomerId").alias("CustomerId"),
            col("Transaction.DateTime").alias("TransactionDateTime"),
            to_date(col("Transaction.DateTime")).alias("TransactionDate"),
            upper(trim(col("Transaction.Location"))).alias("Location"),
            upper(trim(col("Transaction.Result"))).alias("Result"),
            current_timestamp().alias("_processed_insert_timestamp"),
            col('_raw_insert_timestamp').alias('_raw_insert_timestamp')
        ).withColumn("row_rank", row_number().over(windowSpec)) \
            .filter(col("row_rank") == 1) \
            .drop("row_rank")

        #TODO add logic to write malformed documents to DLQ

        # Check for the existence of the Delta table
        if DeltaTable.isDeltaTable(spark, processed):
            # If the table exists, create a DeltaTable instance for it
            delta_table = DeltaTable.forPath(spark, processed)
            # Perform the merge operation
            delta_table.alias("target").merge(
                flattened_df.alias("source"),
                "target.TransactionId = source.TransactionId"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            # If the Delta table does not exist, create one from the batch DataFrame
            flattened_df.write.format("delta").partitionBy("TransactionDate").save(processed)

        print(f"Processed {files_count} files in batch {batch_id}")
    else:
        print("Empty batch")


def stream(spark):
    raw_schema = StructType([
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("topic", StringType()),
        StructField("partition", StringType()),
        StructField("offset", StringType()),
        StructField("timestamp", StringType()),
        StructField("timestampType", StringType()),
        StructField("_raw_insert_timestamp", TimestampType()),
        StructField("_raw_insert_date", StringType()),
        StructField("_raw_insert_hour", IntegerType())
    ])

    df = spark.readStream.format("parquet")\
        .option("path", raw)\
        .schema(raw_schema) \
        .load()
    
    return df.writeStream \
        .foreachBatch(process_batch) \
        .trigger(availableNow=True) \
        .option("checkpointLocation", checkpoint) \
        .start()

    # return df.writeStream \
    #     .outputMode("append") \
    #     .format("console") \
    #     .start()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

retries = 3
retry_num = 0
succeeded = None
while True:
    if retry_num == retries - 1:
        break
    try:
        stream(spark).awaitTermination()
        succeeded = True
        logger.info("Batch processing completed")
        break
    except StreamingQueryException as e:
        retry_num += 1
        # Log the error message
        print(f"Streaming exception:\n{traceback.format_exc()}")
        print("Restarting query after 10 seconds...")
        time.sleep(10)  # Sleep for 10 seconds before restarting the query
    except Exception as e:
        retry_num += 1
        print(f"Non-streaming exception:\n{traceback.format_exc()}")
        print(f"Restarting query after 10 seconds...")        
        time.sleep(10)

if not succeeded:
    logger.error("Batch processing failed")
    pass # TODO send alert that job did not succeed after retries