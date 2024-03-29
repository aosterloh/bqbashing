#!/bin/bash

project="data-academy-2018" # REPLACE project !!!!!!!!!!!!!!!!!!!!!!!
region=EU
NOW=$(date "+%Y.%m.%d-%H.%M.%S")
logfile="log-$NOW.log"
echo "Logging to $logfile"
input=$1

if [ "$#" -ne 1 ]; then
    echo "Wrong num of arguments. Call script along with csv file name, like 'create_datasets input.csv'"
    exit 1
fi

echo -n "This will delete all included tables, are you sure you want to delete? (y/n)? "
read answer
if [ "$answer" == "${answer#[Yy]}" ] ;then
    exit 2
fi

cut -d',' -f1 $1 | sort | uniq > datasets.txt
IFS=$'\n' read -d '' -r -a datasets < datasets.txt

for ds in "${datasets[@]}"
do
  echo "delete dataset $ds" | tee -a $logfile
  bq --project_id="$project" --location="$region" rm -r -f "$ds" 2>&1 | tee -a $logfile
done
