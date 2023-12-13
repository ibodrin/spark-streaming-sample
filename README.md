# spark-streaming-sample

## Purpose
This repository provides sample Spark Structured Streaming application with the use-case of Transaction details sent as XML documents to Apache Kafka and ingested to raw/bronze and processed/silver layers in HDFS.

## Application Logic
1. XML documents are generated by `scripts/misc/generate_xml.py` and stored in HDFS
2. Streaming job `notebooks/main/write_to_kafka.ipynb` is reading XML files and writing them to Kafka topic
3. Streaming job `notebooks/main/write_to_raw.ipynb` is reading files from the Kafka topic and writing them to Parquet table partitioned by ingestion date and hour columns in `Raw` layer on HDFS
4. Scheduled streaming job `notebooks/main/merge_to_processed.ipynb` with `trigger=availableNow` is reading increments from the Parquet table in `Raw` layer and performs necessary data enrichment followed by the upsert into Delta table in `Processed` layer based on regular intervals (e.g. hourly or on-demand)

## Instructions
1. Save `docker-compose.yml` on your workstation
2. Run `docker compose -f "docker-compose.yml" up -d --build` to start the demo container
3. All services and spark jobs will start up automatically (allow 5 minutes to start)
4. Open `http://localhost:8888` in the browser and use the pre-defined `test/test.ipynb` notebook to view the ingested transaction details in `Raw` and `Processed` layers with Jupyter

## Data Format
* Input format - see comment in `scripts/misc/generate_xml.py`
* Output format - see sample output in `notebooks/test/test.ipynb`

## Notes
* When scheduling `scripts/spark/merge_to_processed.py`, ensure that only one running instance of the job is allowed
* `spark-submit` scripts can be found in `scripts/shell`
* Spark UI is available at `http://localhost:8080` and `http://localhost:4040`

## Referenced Documentation
1. https://spark.apache.org/docs/latest/
2. https://kafka.apache.org/documentation/
3. https://hadoop.apache.org/docs/stable/
4. https://github.com/databricks/spark-xml
