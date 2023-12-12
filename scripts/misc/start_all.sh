#!/bin/bash

hdp_path=/opt/hadoop-3.3.6
kafka_path=/opt/kafka_2.13-3.6.1
spark_path=/opt/spark-3.5.0-bin-hadoop3

script_path=$(dirname $0)
cd $script_path
repo_path=$(git rev-parse --show-toplevel)

echo "Starting spark-master"
$spark_path/sbin/start-master.sh
seconds=10 ; echo "Sleeping ${seconds} seconds..." ; sleep ${seconds}
echo "Starting spark-worker"
$spark_path/sbin/start-worker.sh spark://spark-test1:7077 &&

echo "Starting hdfs"
$hdp_path/bin/hdfs namenode -format -force
$hdp_path/bin/hdfs --daemon start namenode
$hdp_path/bin/hdfs --daemon start datanode
echo "Creating directories in hdfs"
$hdp_path/bin/hdfs dfs -mkdir -p \
    /checkpoint/raw/transactions \
    /checkpoint/processed/transactions \
    /raw/transactions /processed/transactions \
    /in/transactions \
    /dlq/processed/transactions \
    /dlq/raw/transactions

echo "Starting kafka zookeper"
rm $kafka_path/logs/*
nohup $kafka_path/bin/zookeeper-server-start.sh $kafka_path/config/zookeeper.properties 2>&1 >/tmp/kafka-zookeper.log &
seconds=30 ; echo "Sleeping ${seconds} seconds..." ; sleep ${seconds}

echo "Starting kafka server"
nohup $kafka_path/bin/kafka-server-start.sh $kafka_path/config/server.properties 2>&1 >/tmp/kafka-server.log &
seconds=10 ; echo "Sleeping ${seconds} seconds..." ; sleep ${seconds}

echo "Starting kafka server"
$kafka_path/bin/kafka-topics.sh --create --topic test-topic --bootstrap-server localhost:9092
seconds=10 ; echo "Sleeping ${seconds} seconds..." ; sleep ${seconds}

# echo "Cleaning up from previous runs"
# $repo_path/scripts/misc/cleanup.sh

echo "Starting spark jobs"
echo "Starting write_to_kafka"
nohup $repo_path/scripts/shell/start_write_to_kafka.sh 2>&1 >/tmp/start_write_to_kafka.log &

echo "Starting write_to_raw"
nohup $repo_path/scripts/shell/start_write_to_raw.sh 2>&1 >/tmp/start_write_to_raw.log &

echo "Starting generate_xml"
nohup $repo_path/scripts/misc/start_generate_xml.sh 2>&1 >/tmp/start_generate_xml.log &

echo "Starting merge_to_processed (1st run)"
nohup $repo_path/scripts/shell/start_merge_to_processed.sh 2>&1 >/tmp/start_merge_to_processed.log &
seconds=60 ; echo "Sleeping ${seconds} seconds..." ; sleep ${seconds}

echo "Starting merge_to_processed (2nd run)"
nohup $repo_path/scripts/shell/start_merge_to_processed.sh 2>&1 >/tmp/start_merge_to_processed.log &

echo "Starting Jupyter"
~/.local/bin/jupyter notebook --generate-config
echo -e "c.NotebookApp.token = ''\nc.NotebookApp.password = ''" >> /home/spark/.jupyter/jupyter_notebook_config.py
nohup ~/.local/bin/jupyter-notebook 2>&1 >/tmp/jupyter-notebook.log &

echo "Startup completed"
