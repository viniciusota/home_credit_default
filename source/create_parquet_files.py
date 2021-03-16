import os
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.types import StructField,StringType,IntegerType,StructType
spark = SparkSession.builder.appName("Basics").getOrCreate()


# Get file names:
list_file_parquet = [file_csv.split(".")[0] for file_csv in os.listdir('data') ]

for read_file,file_parquet in zip( os.listdir('data') , list_file_parquet ):
    # Read csv file with spark:
    data = spark.read.csv( os.path.join(  'data/' , read_file  ) , header = True ) 

    # Create file name to save as parquet: 
    file_name = "{}.parquet".format(file_parquet)
    
    # Create parquet file:
    data.write.parquet( os.path.join( 'parquet_files/' , file_name ) )