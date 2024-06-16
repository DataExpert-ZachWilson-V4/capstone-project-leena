
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

match_num_df1 = intermediate_df.withColumn("input_file_name",input_file_name())

match = match_num_df1.withColumn("input_file_name", f.regexp_extract(f.col('input_file_name'), '[0-9]+', 0)) \
    .withColumn("balls_per_over", col("info.balls_per_over")) \
    .withColumn("dates", explode(col("info.dates"))) \
    .withColumn("match_gender", col("info.gender")) \
    .withColumn("match_type", col("info.match_type")) \
    .withColumn("match_winner", col("info.outcome.winner")) \
    .withColumn("match_season", col("info.season")) \
    .withColumn("match_team_type", col("info.team_type")) \
    .withColumn("team_1", col("info.teams").getItem(0)) \
    .withColumn("team_2", col("info.teams").getItem(1)) \
    .withColumn("player_of_match", explode("info.player_of_match")) \
    .withColumn("toss_decision", col("info.toss.decision")) \
    .withColumn("toss_winner", col("info.toss.winner")) \
    .withColumn("match_venue", col("info.venue")) \
    .withColumn("innings", explode(col("innings"))) \
    .withColumn("overs", explode("innings.overs")) \
    .withColumn("over_number", col("overs.over")) \
    .withColumn("deliveries", explode("overs.deliveries")) \
    .withColumn("batter", col("deliveries.batter")) \
    .withColumn("bowler", col("deliveries.bowler")) \
    .withColumn("non_striker", col("deliveries.non_striker")) \
    .withColumn("run_scored", col("deliveries.runs.total")) \
    .withColumn("wickets", explode("deliveries.wickets")) \
    .withColumn("player_out", col("wickets.player_out")) \
    .withColumn("fielders", explode("wickets.fielders")) \
    .withColumn("fielders_name", col("fielders.name")) \
    .withColumn("kind_out_by", col("wickets.kind")) \
    .drop("info","innings", "deliveries", "overs", "wickets", "fielders","meta")

match.show(truncate=False)