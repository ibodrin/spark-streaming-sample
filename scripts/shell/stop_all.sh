#!/bin/bash

script_path=$(dirname $0)
cd $script_path
repo_path=$(git rev-parse --show-toplevel)

$repo_path/scripts/misc/stop_generate_xml.sh
$repo_path/scripts/shell/stop_merge_to_processed.sh
$repo_path/scripts/shell/stop_write_to_raw.sh
$repo_path/scripts/shell/stop_write_to_kafka.sh
/opt/kafka_2.13-3.6.1/bin/zookeeper-server-stop.sh
/opt/kafka_2.13-3.6.1/bin/kafka-server-stop.sh
/opt/hadoop-3.3.6/sbin/stop-dfs.sh
/opt/hadoop-3.3.6/sbin/stop-yarn.sh
