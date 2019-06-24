#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Wrong num of arguments. Call script along with timestamp. Show all jobs that ran after the timestamp, e.g. ./status 1560645900000 "
    exit 1
fi

echo "Overview of all jobs: "
bq ls -j -a -n 2000 --min_creation_time $1 | grep bqjob | tee -a $log_jobs.txt

jobs=0
failed=0
running=0
pending=0
success=0
while read line 
do
	jobs=$((jobs+1))
	if [[ $line =~ "SUCCESS" ]]; then
		success=$((success+1))
	elif [[ $line =~ "FAILURE" ]];  then
		failure=$((failure+1))
	elif [[ $line =~ "PENDING" ]]; then
		pending=$((pending+1))
	elif [[ $line =~ "RUNNING" ]]; then
		running=$((running+1))
	fi
done < log_jobs.txt



echo "Total number of submitted jobs: $jobs"
echo ""
echo "Number of jobs in status 'RUNNING': $running"
echo ""
echo "Number of jobs in status 'SUCCESS': $success"
echo ""
echo "Number of jobs in status 'PENDING': $pending"
echo ""
echo "Number of jobs in status 'FAILURE': $failure"
