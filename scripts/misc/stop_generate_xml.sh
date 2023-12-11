#!/bin/bash

job_name=generate_xml

if [ -f /tmp/${job_name}.pid ] ; then
    pid=$(cat /tmp/${job_name}.pid)
fi
if ! [ -z "$pid" ] ; then
    kill -9 $pid
    echo "Killed job ${generate_xml} with pid ${pid}"
else 
    echo "Job ${job_name} is not running"
fi
