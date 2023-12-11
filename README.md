# spark-streaming-sample

## Purpose
This repository provides sample Spark Structured Streaming application with the use-case of Transaction details sent as XML documents to Apache Kafka and ingested to raw/bronze and processed/silver layers in HDFS.

## Application Logic
1. XML documents are generated by `scripts/misc/generate_xml.py` and stored in HDFS
2. Streaming job `scripts/spark/write_to_kafka.py` is reading XML files and writing them to Kafka topic
3. Streaming job `scripts/spark/write_to_raw.py` is reading files from the Kafka topic and writing them to Parquet table partitioned by ingestion date and hour columns in `Raw` layer on HDFS
4. Scheduled streaming job `scripts/spark/merge_to_processed.py` with `trigger=availableNow` is reading increments from the Parquet table in `Raw` layer and performs necessary data enrichment followed by the upsert into Delta table in `Processed` layer based on regular intervals (e.g. hourly or on-demand)

## Instructions
1. Start demo container using `docker-compose.yml`
2. Upon the container startup, all services and spark jobs will start automatically
3. Connect using Remote-SSH in VSCode as `spark@spark-test1` on port `22203` and start SSH tunnel on ports `8080` and `4040` to browse Spark UI
5. Use Jupyter extension in VSCode to execute `notebooks/test.ipynb` and browse ingested transaction details in `Raw` and `Processed` layers

## Notes
* When scheduling `scripts/spark/merge_to_processed.py`, ensure that only one running instance of the job is allowed
* `spark-submit` scripts can be found in `scripts/shell`

## Referenced Documentation
1. https://spark.apache.org/docs/latest/
2. https://kafka.apache.org/documentation/
3. https://hadoop.apache.org/docs/stable/
4. https://github.com/databricks/spark-xml
