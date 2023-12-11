#!/bin/bash

job_name=write_to_kafka
spark-submit --kill write_to_kafka --master spark://spark-test1:7077
