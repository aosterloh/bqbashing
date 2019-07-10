#!/bin/bash

input=$1

if [ "$#" -ne 1 ]; then
    echo "Wrong num of arguments. Call script along with skip file name'"
    exit 1
fi

while IFS=',' read -r  dname tname uri
do
    #echo "Checking $uri "
    gcsexist=$(gsutil -q stat $uri ; echo $? )
    if [ $gcsexist -eq 0 ]; then
      #load the data
      echo "$uri EXISTS" 
      echo " "
    fi
done < $input

