# bqbashing

loadv.sh - uses bq load to load parquet files from GCS, using a csv file (all jobs are queued on BQ side)

How to use:
1. create csv as script input, csv file must have 3 columns: dataset name,table name,gcs path to parquet file 
   e.g. customer, sales, gs://customer/parquet/sales.parquet
2. put csv file in same folder as scripts and if needed run dos2unix to clean up 
3. edit project variable in all scripts
4. optional: if you ran step 5 before, delete_datasets.sh <csv file> deletes all datasets + tables (hence use with caution)
5. run create_datasets.sh <csv file> - uses created csv file to create all datasets  
6. run load.sh <csv file> to start the batch load job
7. in new terminal window run status.sh <timestamp ms> where timestamp is in milliseconds of time just before you started load job (script will only show jobs that ran after that time) use e.g. https://www.epochconverter.com/ to convert

