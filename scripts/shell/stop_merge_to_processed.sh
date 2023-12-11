#!/bin/bash

job_name=merge_to_processed

if [ -f /tmp/${job_name}.pid ] ; then
    pid=$(cat /tmp/${job_name}.pid)
fi
if ! [ -z "$pid" ] ; then
    kill -9 $pid
    echo "Killed job ${job_name} with pid ${pid}"
else 
    echo "Job ${job_name} is not running"
fi
