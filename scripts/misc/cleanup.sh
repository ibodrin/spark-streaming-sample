#!/bin/bash

/opt/kafka_2.13-3.6.1/bin/kafka-topics.sh --delete --topic test-topic --bootstrap-server localhost:9092
/opt/hadoop-3.3.6/bin/hdfs dfs -rm -r /checkpoint /raw /processed /in /dlq
/opt/hadoop-3.3.6/bin/hdfs dfs -mkdir -p \
    /checkpoint/raw/transactions \
    /checkpoint/processed/transactions \
    /raw/transactions /processed/transactions \
    /in/transactions \
    /dlq/processed/transactions \
    /dlq/raw/transactions
/opt/kafka_2.13-3.6.1/bin/kafka-topics.sh --create --topic test-topic --bootstrap-server localhost:9092
