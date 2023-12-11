#!/bin/bash

hdp_path=/opt/hadoop-3.3.6
kafka_path=/opt/kafka_2.13-3.6.1

script_path=$(dirname $0)
cd $script_path
repo_path=$(git rev-parse --show-toplevel)

rm $kafka_path/logs/*

echo "Starting yarn"
nohup $hdp_path/sbin/start-yarn.sh 2>&1 >/tmp/hadoop-yarn.log &
seconds=60 ; echo "Sleeping ${seconds} seconds..." ; sleep ${seconds}

echo "Starting dfs"
nohup $hdp_path/sbin/start-dfs.sh 2>&1 >/tmp/hadoop-dfs.log &

echo "Starting kafka zookeper"
nohup $kafka_path/bin/zookeeper-server-start.sh $kafka_path/config/zookeeper.properties 2>&1 >/tmp/kafka-zookeper.log &
seconds=10 ; echo "Sleeping ${seconds} seconds..." ; sleep ${seconds}

echo "Starting kafka server"
nohup $kafka_path/bin/kafka-server-start.sh $kafka_path/config/server.properties 2>&1 >/tmp/kafka-server.log &
seconds=10 ; echo "Sleeping ${seconds} seconds..." ; sleep ${seconds}

echo "Cleaning up from previous runs"
$repo_path/scripts/misc/cleanup.sh

echo "Starting write_to_kafka"
nohup $repo_path/scripts/shell/start_write_to_kafka.sh 2>&1 >/tmp/start_write_to_kafka.log &
echo "Starting write_to_raw"
nohup $repo_path/scripts/shell/start_write_to_raw.sh 2>&1 >/tmp/start_write_to_raw.log &
echo "Starting generate_xml"
nohup $repo_path/scripts/misc/start_generate_xml.sh 2>&1 >/tmp/start_write_to_raw.log &
seconds=60 ; echo "Sleeping ${seconds} seconds..." ; sleep ${seconds}
nohup $repo_path/scripts/shell/start_merge_to_processed.sh 2>&1 >/tmp/start_merge_to_processed.log &
