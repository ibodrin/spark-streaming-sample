#!/bin/bash

job_name=merge_to_processed
spark-submit --kill merge_to_processed --master spark://spark-test1:7077
