#!/bin/bash
cd /opt/kafka_2.13-3.6.1
echo "Stopping Kafka"
bin/zookeeper-server-stop.sh
sleep 10
bin/kafka-server-stop.sh
sleep 10
echo "Starting Kafka"
bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
sleep 10
nohup bin/kafka-server-start.sh -daemon config/server.properties
