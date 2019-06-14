#!/bin/bash
#expecting comma separated csv with parquet files as argument with 3 columns:
#dataset_name,table_name,GCS_URI

# 3 settings to override 
project="your project" # REPLACE project !!!!!!!!!!!!!!!!!!!!!!!
region="EU"
replace="false" #true overwrites existing table, false is write_append


tables=0
datasets=0
line=1
start=`date +%s`
NOW=$(date "+%Y.%m.%d-%H.%M.%S")
logfile="log-$NOW.log"
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
    echo "URI: $uri "
    echo "dname: $dname"
    echo "tname: $tname"
    # check if URL is valid
    gcsexist=$(gsutil ls $uri)
    if [[ $gcsexist =~ "parquet" ]]; then
      #create dataset if not exists
      echo "URI is valid" | tee -a $logfile
      exists=$(bq --project_id="$project" --location="$region" ls -d | grep -w $dname) 

      if [ -n "$exists" ]; then
         echo "Not creating dataset as $dname already exists" | tee -a $logfile
      else
         echo "Creating new dataset $dname" | tee -a $logfile
         bq --project_id="$project" --location="$region" mk "$dname" 2>&1 | tee -a $logfile
         #rc=$? #immediatly after
         ((datasets+=1))
      fi
      #load the data
      bq --project_id="$project"  --location="$region" load --replace="$replace" --source_format=PARQUET "$dname"."$tname" "$uri"  2>&1 | tee -a $logfile
      ((tables+=1))
    else 
    # uri not valid, hence skip and log it  
    echo "Skipping import of table $tname as URI $uri cannot be found" | tee -a $logfile
    echo "$dname,$tname,$uri" >> skipped-files.csv
    fi
  ((line+=1))
  echo " "
done < $input

end=`date +%s` #to calculate how long it took in total

echo "Logging to $logfile"
echo "Import took $((end-start)) seconds" | tee -a $logfile
echo "Done importing data for project $project in region $region" | tee -a $logfile
echo "Using CSV $input" | tee -a $logfile
echo "Created $datasets new datasets" | tee -a $logfile
echo "Created $tables tables" | tee -a $logfile

