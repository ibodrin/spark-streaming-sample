#!/bin/sh

cd /opt/hadoop-3.3.6
echo "Stopping HDFS"
sbin/stop-dfs.sh
sbin/stop-yarn.sh
echo "Starting HDFS"
nohup sbin/start-yarn.sh 2>&1 >/tmp/hadoop-yarn.log &
nohup sbin/start-dfs.sh 2>&1 >/tmp/hadoop-dfs.log &
