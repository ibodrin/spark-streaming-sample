#!/bin/bash

job_name=generate_xml

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

nohup python3 $repo_path/scripts/misc/generate_xml.py 2>&1 >/tmp/${job_name}.log &

pid=$!
echo $pid > /tmp/${job_name}.pid
echo "Job ${job_name} started with pid ${pid}, output redirected to /tmp/${job_name}.log"
