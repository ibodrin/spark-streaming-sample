#!/bin/bash

job_name=write_to_raw
spark-submit --kill write_to_raw --master spark://spark-test1:7077
