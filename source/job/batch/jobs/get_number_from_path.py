# This section is responisble for reading all jsons and saving df
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, input_file_name
from pyspark.sql.types import *
import pyspark.sql.functions as f

# Initialize Spark session
spark = SparkSession.builder.appName("JSONFlatten").getOrCreate()

# Load the JSON file into a DataFrame
df = spark.read.format('json').option("multiline", True).load("/Users/leenapatil/Documents/all_json/64013.json")
# match_num_df = df.withColumn("input_file_name",f.split(f.input_file_name(), '/')[4])

match_num_df = df.withColumn("input_file_name",input_file_name())

new_df = match_num_df.withColumn('input_file_name', f.regexp_extract(f.col('input_file_name'), '[0-9]+', 0))


new_df.select("input_file_name").show(truncate=False)