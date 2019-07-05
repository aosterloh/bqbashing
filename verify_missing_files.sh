#!/bin/bash

input=$1

if [ "$#" -ne 1 ]; then
    echo "Wrong num of arguments. Call script along with skip file name'"
    exit 1
fi

while IFS=',' read -r  dname tname uri
do
    echo "Checking $uri "
    gcsexist=$(gsutil -q stat $uri ; echo $? )
    if [ $gcsexist -eq 0 ]; then
      #load the data
      echo "$uri EXISTS EXISTS EXISTS EXISTS EXISTS" 
      echo " "
    else 
	    # uri not valid, hence skip and log it  
	    echo "$uri does not exist" 
	    echo " "
    fi
done < $input

