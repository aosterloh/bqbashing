#!/bin/bash
#expecting comma separated file with 3 columns:
#dataset_name,table_name,GCS_URI
input="input.csv" #table list from customer
project="xxxx"
region="EU"
replace="false" #true overwrites existing table, false is write_append

tables=0
datasets=0

while IFS=',' read -r dname tname uri
do
  gcsexist=$(gsutil stat $uri)
    if [[ $gcsexist == *"Creation time"* ]]; then
      #create dataset if not exists
      exists=$(bq ls -d | grep -w $dname)
      if [ -n "$exists" ]; then
         echo "Not creating dataset $dname since it already exists"
      else
         echo "Creating dataset $dname"
         bq --project_id="$project" --location="$region" mk "$dname"
         ((datasets+=1))
      fi

      bq --project_id="$project"  --location="$region" load --replace="$replace" --source_format=PARQUET "$dname"."$tname" "$uri"
      ((tables+=1))
  else 
      echo "Skipping import of table $tname as URI $uri cannot be found"
  fi
      
done < "$input"

echo "Done importing data for project $project in region $region"
echo "Using CSV $input"
echo "Created $datasets new datasets"
echo "Created $tables tables using replace = $replace"
