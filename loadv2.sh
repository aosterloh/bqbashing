#!/bin/bash
#expecting comma separated csv as argument with 3 columns:
#dataset_name,table_name,GCS_URI

# REPLACE project !!!!!!!!!!!!!!!!!!!!!!!
project="data-academy-2018"
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

# takes 1 argument, the csv file name, e.g. loadv2.sh input.csv
if [ "$#" -ne 1 ]; then
    echo "call script along with csv file name, like './loadv2.sh input.csv'"
    exit 1
fi


echo "Starting new import at $date \n" > $logfile
while IFS=',' read -r dname tname uri
do
    echo " "
    echo "reading $line line with uri $uri "
    gcsexist=$(gsutil stat $uri)
    
    
    if [[ $gcsexist == *"Creation time"* ]]; then
      #create dataset if not exists
      echo "file $uri exists" >> $logfile
      exists=$(bq --project_id="$project" --location="$region" ls -d | grep -w $dname)
      echo "Exists = $exists" 

      if [ -n "$exists" ]; then
         echo "Not creating dataset $dname since it already exists\n" >> $logfile
      else
         echo "Creating dataset $dname\n" >> import.log
         bq --project_id="$project" --location="$region" mk "$dname" >> $logfile
         #rc=$? #immediatly after
         ((datasets+=1))
      fi

      bq --project_id="$project"  --location="$region" load --replace="$replace" --source_format=PARQUET "$dname"."$tname" "$uri"  >> $logfile
      ((tables+=1))
    else 
    echo "Skipping import of table $tname as URI $uri cannot be found\n" >> $logfile
    fi
  ((line+=1))     
done < $input

end=`date +%s`

echo "Import took $((end-start)) seconds" >> $logfile
echo "Done importing data for project $project in region $region" >> $logfile
echo "Using CSV $input" >> $logfile
echo "Created $datasets new datasets" >> $logfile
echo "Created $tables tables using replace = $replace" >> $logfile
echo "Import took $((end-start)) seconds" 
echo "Done importing data for project $project in region $region" 
echo "Using CSV $input"
echo "Created $datasets new datasets" 
echo "Created $tables tables using replace = $replace" 
