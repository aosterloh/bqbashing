#!/bin/bash
#expecting comma separated csv with parquet files as argument with 3 columns:
#dataset_name,table_name,GCS_URI

# 3 settings to override 
project="p-id" # REPLACE project !!!!!!!!!!!!!!!!!!!!!!!
region="EU"
replace="true" #true overwrites existing table, false is write_append
tables=0
datasets=0
start=`date +%s`
NOW=$(date "+%Y.%m.%d-%H.%M.%S")
logfile="log-$NOW.log"
skipfile="skip-$NOW.log"
echo "Logging to $logfile"
input=$1

# takes csv file name as argument, e.g. load.sh input.csv
# verify using cat -t input.csv that you don't have special character at the end (^M)
# otherwise remove using e.g. dos2unix
if [ "$#" -ne 1 ]; then
    echo "Wrong num of arguments. Call script along with csv file name, like './loadv2.sh input.csv'"
    exit 1
fi


while IFS=',' read -r  dname tname uri
do
    echo "Checking $uri "
    #echo "dname: $dname"
    #echo "tname: $tname"
    # check if URL is valid
    gcsexist=$(gsutil ls $uri)
    if [[ $gcsexist =~ "parquet" ]]; then
      #load the data
      bq --project_id="$project"  --location="$region" load --replace="$replace" --source_format=PARQUET "$dname"."$tname" "$uri" &  2>&1 | tee -a $logfile
      ((tables+=1))
    else 
    # uri not valid, hence skip and log it  
    echo "Skipping import of table $tname as URI $uri cannot be found" | tee -a $logfile
    echo "$dname,$tname,$uri" >> $skipfile
    fi

  echo " "
done < $input

end=`date +%s` #to calculate how long it took in total

echo "Logging to $logfile"
echo "Import took $((end-start)) seconds" | tee -a $logfile
echo "Done importing data for project $project in region $region" | tee -a $logfile
echo "Using CSV $input" | tee -a $logfile
echo "Created $datasets new datasets" | tee -a $logfile
echo "Created $tables tables" | tee -a $logfile

