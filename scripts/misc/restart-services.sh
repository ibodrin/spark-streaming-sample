#!/bin/sh

cd $(dirname "$0")
./restart-kafka.sh
./restart-hdfs.sh
