from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, input_file_name
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.functions import arrays_zip, col, explode, map_keys, map_values, explode, col, map_entries
import schema

# Initialize Spark session
spark = SparkSession.builder.appName("dim_player_registry").getOrCreate()

spark.sql.caseSensitive: True
schema = schema.get_schema()
matches_df = spark.read.option("multiline", "true").schema(schema).json(
    "/Users/leenapatil/Documents/all_json/*.json")



df = df.withColumn("file_name", input_file_name()) \
                 .withColumn("match_start_date", col("info.dates").getItem(0)) \
                 .withColumn("match_gender", col("info.gender")) \
                 .withColumn("match_type", col("info.match_type")) \
                 .withColumn("match_winner", col("info.outcome.winner")) \
                 .withColumn("match_season", col("info.season")) \
                 .withColumn("match_team_type", col("info.team_type")) \
                 .withColumn("team_1", col("info.teams").getItem(0)) \
                 .withColumn("team_2", col("info.teams").getItem(1)) \
                 .withColumn("toss_decision", col("info.toss.decision")) \
                 .withColumn("toss_winner", col("info.toss.winner")) \
                 .withColumn("match_venue", col("info.venue"))
match_df = df.withColumn('match_num', f.regexp_extract(f.col('file_name'), '[0-9]+', 0))

match_info = match_df.repartition(4).select("match_num","match_start_date", "match_gender", "match_type","match_winner","match_season","match_team_type","team_1","team_2","toss_decision","toss_winner", "match_venue").write.parquet("/Users/leenapatil/IdeaProjects/capstone-project-leena/data/dim_match")
