#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Wrong num of arguments. Call script along with timestamp. Show all jobs that ran after the timestamp, e.g. ./status 1560645900000 "
    exit 1
fi

echo "Overview of all jobs: "
bq ls -j -a -n 2000 --min_creation_time $1 | grep bqjob
echo ""
echo "Total number of submitted jobs:"
bq ls -j -a -n 2000 --min_creation_time $1 | grep bqjob | wc -l 
echo ""
echo "Number of jobs in status 'RUNNING'"
bq ls -j -a -n 2000 --min_creation_time $1 | grep RUNNING | wc -l 
echo ""
echo "Number of jobs in status 'SUCCESS'"
bq ls -j -a -n 2000 --min_creation_time $1 | grep SUCCESS | wc -l 
echo ""
echo "Number of jobs in status 'PENDING'"
bq ls -j -a -n 2000 --min_creation_time $1 | grep PENDING | wc -l 
echo ""
echo "Number of jobs in status 'FAILURE'"
bq ls -j -a -n 2000 --min_creation_time $1 | grep FAILURE | wc -l 
echo ""