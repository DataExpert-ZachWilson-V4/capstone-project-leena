# This section is responisble for reading all jsons and saving df
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, input_file_name
from pyspark.sql.types import *

spark = SparkSession.builder.appName("dim_player_registry").getOrCreate()

spark.sql.caseSensitive: True
schema = schema.get_schema()
df = spark.read.option("multiline", "true").schema(schema).json(
    "/Users/leenapatil/Documents/all_json/*.json")


df = df.withColumn("file_name", input_file_name()) \
    .withColumn("match_start_date", col("info.dates").getItem(0)) \
    .withColumn("innings", explode(col("innings"))) \
    .withColumn("overs", explode("innings.overs")) \
    .withColumn("team_batting", col("innings.team")) \
    .withColumn("over_number", col("overs.over")) \
    .withColumn("deliveries", explode("overs.deliveries")) \
    .withColumn("batter_name", col("deliveries.batter")) \
    .withColumn("bowler_name", col("deliveries.bowler")) \
    .withColumn("non_striker_name", col("deliveries.non_striker")) \
    .withColumn("runs_per_over", col("deliveries.runs.total")) \
    .withColumn("wickets", explode("deliveries.wickets")) \
    .withColumn("player_out", col("wickets.player_out")) \
    .withColumn("fielders", explode("wickets.fielders")) \
    .withColumn("fielders_name", col("fielders.name")) \
    .withColumn("wickets_kind", col("wickets.kind")) \
    .drop("meta","info","innings","overs","wickets","deliveries","fielders")
deliveries_df = df.withColumn('match_num', f.regexp_extract(f.col('file_name'), '[0-9]+', 0))



deliveries_df.repartition(4).spark.select("match_num","fielders_name","player_out","wickets_kind","non_striker_name","runs_per_over","match_start_date","team_batting","over_number","batter_name","bowler_name").write.parquet("/Users/leenapatil/IdeaProjects/capstone-project-leena/data/dim_deliveries")
