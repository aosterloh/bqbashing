


from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import pandas as pd
import datetime
from pyspark.sql import functions as F 

#sc = SparkContext('local')
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


# In[ ]:


# read csv with parquet files that bq load will not process 
csv_parquet = "gs://kf-eu/broken/broken_parquet2.csv"
fixed_bucket = "gs://kf-eu/fixed/"


csv = pd.read_csv(csv_parquet, usecols=[2])


for index, row in csv.iterrows():
    # access data using column names
    url = row['url']
    file_name = url.rsplit('/', 1)[-1]
    print ("Transforming " + file_name)
    df = spark.read.parquet(url) # load parquet file into a spark dataframe
    #df.printSchema() # Check format of the dat_bis, it's in a datetime format 
    #df.select('dat_bis').show()
    # Find the records that have dates with a year, larger than the Maxyear of 9999
    if 'dat_bis' in df.columns:
        max_date = datetime.date(datetime.MAXYEAR,1,1)
        bad_rows = df['dat_bis'] >= max_date
        good_rows = df['dat_bis'] < max_date
        print "Bad records: {} Good records: {}".format(df.filter(bad_rows).count(), df.filter(good_rows).count())
        # Replace invalid date with the max allowed date, minus a year (9998-1-1)
        max_allowed_date = datetime.date(datetime.MAXYEAR - 1, 1, 1) 
        df_fixed = df.withColumn('dat_bis', F.when(F.col('dat_bis') >= max_date, max_allowed_date).otherwise(F.col('dat_bis')))
        df_repartitioned = df_fixed.repartition(1)
        output_file=fixed_bucket+file_name
        print "Writing to "
        print output_file
        file = df_repartitioned.coalesce(1).write.format("parquet").save(output_file)
        #file = df_fixed.write.format("parquet").save(output_file)
        print "Done with " + file_name
        df_fixed.unpersist()
        df_repartitioned.unpersist()
    else:
        print ("Skipping transformation for " + file_name + " because dat_bis does not exist")
    df.unpersist()
    

