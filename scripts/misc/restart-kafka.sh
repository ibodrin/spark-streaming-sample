#!/bin/bash
cd /opt/kafka_2.13-3.6.1
bin/kafka-server-stop.sh
bin/zookeeper-server-stop.sh
nohup bin/zookeeper-server-start.sh config/zookeeper.properties 2>&1 >/tmp/kafka-zookeper.log &
nohup bin/kafka-server-start.sh config/server.properties 2>&1 >/tmp/kafka-server.log &