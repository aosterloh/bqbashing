# bqbashing

loadv2.sh - uses bq load to queue loading parquet files from GCS, using a csv file as script input 
csv file has 3 columns: dataset_name,table_name,parquet_uri

create_datasets.sh - uses same csv file to get all dataset names to create before running load2.sh 

delete_datasets.sh - deletes all datasets + tables from the csv file (use with caution)

status.sh - takes timestamp in milliseconds (before you ran the loadv2.sh script) to show status of all submitted load jobs
