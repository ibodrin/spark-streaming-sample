#!/bin/bash

job_name=write_to_kafka
spark_path=/opt/spark-3.5.0-bin-hadoop3

if [ -f /tmp/${job_name}.pid ] ; then
  pid=$(cat /tmp/${job_name}.pid)
  status="$(ps -p $(cat /tmp/${job_name}.pid))"
  if ! [ "$(echo "$status" | wc -l)" == "1" ] && ! echo "$status" | grep -q defunct; then
    echo "Job ${job_name} with PID ${pid} is already running."
    exit 0  # Exit with success status code as we're intentionally skipping.
  fi
fi

script_path=$(dirname $0)
cd $script_path
repo_path=$(git rev-parse --show-toplevel)

nohup $spark_path/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --master spark://spark-test1:7077 \
  --executor-memory 512m \
  $repo_path/scripts/spark/${job_name}.py 2>&1 > /tmp/${job_name}.log &

# spark-submit \
#   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
#   --master spark://spark-test1:7077 \
#   ../spark/${job_name}.py 2>&1 > /tmp/${job_name}.log

pid=$!
echo $pid > /tmp/${job_name}.pid
echo "Job ${job_name} started with pid ${pid}, output redirected to /tmp/${job_name}.log"
