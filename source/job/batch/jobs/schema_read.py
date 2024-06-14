
# This section is responisble for reading all jsons and saving df
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, input_file_name
from pyspark.sql.types import *
import pyspark.sql.functions as f

# Initialize Spark session
spark = SparkSession.builder.appName("JSONFlatten").getOrCreate()

spark.sql.caseSensitive: True

# Few columns were coming as duplicate in raw file. e.g.: languages[0].groupingsets[0].element.attributes.tags[0] was repeated twice.
# This caused errror while creating dataframe.
# However, we are able to read it in Databricks Runtime 7.3 LTS. Hence used this runtime to read a file and write it to ADLS as ONE-TIME activity.
# For all further runs, this file can be read using multiline as false, then use its schema while reading the other new files (which in this case needs multiline as true). In this way spark does not have to create schema on its own hence does not throw error eben in higher runtime versions.
# Have used a historical file initially delivered which had a lot of records due to historical data. This ensures we cover all possibilities.
# Can be created again using 7.3 LTS runtime cluster if this schema is deleted.

dfOldRuntime = spark.read.option("multiline","true").json("/Users/leenapatil/Documents/all_json/207334.json") # Can take any file to creat sample schema.
dfOldRuntime.coalesce(1).write.mode('overwrite').format('json').save("/Users/leenapatil/Documents/schema/")

# Read sample which was created using 7.3 LTS runtime.
# The multiline does NOT have to be true for this.
# Get its schema and use it to read new files even on higher runtime without error which was caused due to duplicate columns.
dfSchema = spark.read.json("/Users/leenapatil/Documents/schema/")
schema = dfSchema.schema

# Read new json files using this schema by using `.schema()`. Works on higher runtimes as well since spark now does not have to create schema on its own.
intermediate_df = spark.read.option("multiline","true").schema(schema).json("/Users/leenapatil/Documents/all_json/1*.json")

match_num_df = intermediate_df.withColumn("input_file_name",input_file_name())
new_df = match_num_df.withColumn('input_file_name', f.regexp_extract(f.col('input_file_name'), '[0-9]+', 0))

innings_df = new_df.withColumn("innings", explode(col("innings")))

overs_df = innings_df.withColumn("overs", explode("innings.overs"))

deliveries_df = overs_df.withColumn("deliveries", explode("overs.deliveries"))
data1_df = deliveries_df.select("innings.team","innings.declared","deliveries.batter","deliveries.bowler", "deliveries.runs.total","deliveries.wickets")

#data1_df.coalesce(4).write.format("parquet").save("/Users/leenapatil/Documents/match_deliveries_fact_table/")
