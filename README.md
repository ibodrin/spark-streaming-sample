# spark-streaming-sample

## Purpose
This repository stores a working example of Spark Structured Streaming application with a use-case of Transaction details sent as XML documents to Apache Kafka.

## Application Logic
1. XML documents are generated by `scripts/misc/generate_xml.py` and stored in HDFS
2. Streaming job `scripts/spark/write_to_kafka.py` is reading XML files and writing them to Kafka topic
3. Streaming job `scripts/spark/write_to_raw.py` is reading files from the Kafka topic and writing them to Parquet table in `Raw` layer on HDFS
4. Scheduled streaming job `scripts/spark/merge_to_processed.py` with `trigger=availableNow` is reading increments from the Parquet table and performs necessary data enrichment followed by upsert into Delta table in `Processed` layer based on regular intervals (hourly or on-demand)

## Instructions
1. Start container using `docker-compose.yml` _(TBD push image to Docker Hub)_
2. Connect using Remote-SSH in VSCode as `spark@spark-test1`
3. Use Jupyter extension in VSCode to execute `notebooks/test.ipynb` and browse ingested transaction details in `Raw` and `Processed` layers

## Notes
* When scheduling `scripts/spark/merge_to_processed.py`, ensure that only one instance of the job is allowed
