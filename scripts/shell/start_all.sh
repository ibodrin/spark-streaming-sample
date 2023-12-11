#!/bin/bash

hdp_path=/opt/hadoop-3.3.6
kafka_path=/opt/kafka_2.13-3.6.1

script_path=$(dirname $0)
cd $script_path
repo_path=$(git rev-parse --show-toplevel)

echo "Starting yarn"
nohup $hdp_path/sbin/start-yarn.sh 2>&1 >/tmp/hadoop-yarn.log &
seconds=10 ; echo "Sleeping ${seconds} seconds..." ; sleep ${seconds}

echo "Starting dfs"
nohup $hdp_path/sbin/start-dfs.sh 2>&1 >/tmp/hadoop-dfs.log &

echo "Starting kafka zookeper"
$kafka_path/bin/zookeeper-server-start.sh -daemon $kafka_path/config/zookeeper.properties
seconds=10 ; echo "Sleeping ${seconds} seconds..." ; sleep ${seconds}

echo "Starting kafka server"
$kafka_path/bin/kafka-server-start.sh -daemon $kafka_path/config/server.properties
seconds=30 ; echo "Sleeping ${seconds} seconds..." ; sleep ${seconds}

echo "Cleaning up"
$repo_path/scripts/misc/cleanup.sh

echo "Starting spark jobs"
nohup $repo_path/scripts/shell/start_write_to_kafka.sh 2>&1 >/tmp/start_write_to_kafka.log &
nohup $repo_path/scripts/shell/start_write_to_raw.sh 2>&1 >/tmp/start_write_to_raw.log &
nohup $repo_path/scripts/shell/start_merge_to_processed.sh 2>&1 >/tmp/start_merge_to_processed.log &
