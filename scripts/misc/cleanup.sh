#!/bin/bash

/opt/kafka_2.13-3.6.1/bin/kafka-topics.sh --delete --topic test-topic --bootstrap-server localhost:9092
/opt/hadoop-3.3.6/bin/hdfs dfs -rm -r /checkpoint /raw /processed /in /dlq
/opt/hadoop-3.3.6/bin/hdfs dfs -mkdir -p /checkpoint/raw /checkpoint/processed /raw /processed /in/transactions /dlq/processed /dlq/raw
/opt/kafka_2.13-3.6.1/bin/kafka-topics.sh --create --topic test-topic --bootstrap-server localhost:9092
