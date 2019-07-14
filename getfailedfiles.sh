#!/bin/bash
# Get list of all bq load jobs that failed after timestamp in ms (argument)
# Script get's all parquet files of failed load jobs and writes csv file: broken_parquet.csv
# This file is input for spark job that reads parquet files, fixes date value and writes out fixed parquet file

#change this one
input_csv=csv-jul3-clean.csv 

# Don't change anything else
input=$1
tempfile=failed_jobs.log
broken_parquet=broken_parquet.csv
jobids=jobids.log

rm $tempfile
rm $broken_parquet


if [ "$#" -ne 1 ]; then
    echo "Wrong num of arguments. Call script along with timestamp in ms (only consider jobs after that time)"
    exit 1
fi

bq ls -j -a -n 1200 --min_creation_time $1 | grep FAILURE >> $tempfile 


while IFS=' ' read -r  id t2 t3 t4 t5 t6 t7 t8 t9 t10
do
    
    output=$(bq show -j $id | grep ".parquet" | cut -d' ' -f 3 |  tr -d :)
    cat $input_csv | grep $output | tee -a $broken_parquet 
    

done < $tempfile

