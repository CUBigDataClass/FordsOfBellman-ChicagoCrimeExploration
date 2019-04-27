from google.cloud import storage
storage_client=storage.Client()
bucket=storage_client.get_bucket('chic_crime')
blobs=list(bucket.list_blobs())
print(blobs)
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Chicago_crime_analysis").getOrCreate()
from pyspark.sql.types import  (StructType,StructField,DateType,BooleanType,DoubleType,IntegerType,StringType,TimestampType)
crimes = spark.read.csv("ccd_sample.csv",header = True,schema = crimes_schema)
print(" The crimes dataframe has {} records".format(crimes.count()))
print(crimes.select("PrimaryType").distinct().show(n = 5))