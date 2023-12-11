#!/bin/sh

cd /opt/hadoop-3.3.6
sbin/stop-dfs.sh
sbin/stop-yarn.sh
nohup sbin/start-yarn.sh 2>&1 >/tmp/hadoop-yarn.log &
nohup sbin/start-dfs.sh 2>&1 >/tmp/hadoop-dfs.log &
