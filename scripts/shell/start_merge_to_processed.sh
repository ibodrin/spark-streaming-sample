#!/bin/bash

job_name=merge_to_processed

if [ -f /tmp/${job_name}.pid ] ; then
  pid=$(cat /tmp/${job_name}.pid)
  status="$(ps -p $(cat /tmp/${job_name}.pid))"
  if ! [ "$(echo "$status" | wc -l)" == "1" ] && ! echo "$status" | grep -q defunct; then
    echo "Job ${job_name} with PID ${pid} is already running."
    exit 0  # Exit with success status code as we're intentionally skipping.
  fi
fi

cd $(dirname "$0")

nohup spark-submit \
    --name "MergeToProcessed" \
    --master "spark://spark-test1:7077" \
    --packages "io.delta:delta-spark_2.12:3.0.0,com.databricks:spark-xml_2.12:0.14.0" \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    --conf "spark.cores.max=1" \
    --executor-memory 512m \
    ../spark/${job_name}.py 2>&1 > /tmp/${job_name}.log &

# spark-submit \
#   --name "MergeToProcessed" \
#   --master "spark://spark-test1:7077" \
#   --packages "io.delta:delta-spark_2.12:3.0.0,com.databricks:spark-xml_2.12:0.14.0" \
#   --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
#   --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
#   --conf "spark.cores.max=1" \
#   ../spark/${job_name}.py 2>&1 > /tmp/${job_name}.log

pid=$!
echo $pid > /tmp/${job_name}.pid
echo "Job ${job_name} started with pid ${pid}, output redirected to /tmp/${job_name}.log"