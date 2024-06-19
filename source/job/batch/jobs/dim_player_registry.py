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
matches_df = matches_df.withColumn("file_name", input_file_name())
people_df = matches_df.select("file_name", "info.*").withColumn("player_info",
                                                                explode(map_entries(col("registry.people")))) \
    .selectExpr("*", "player_info.key as player_name", "player_info.value as player_id") \
    .drop("registry")
match_palyer_registry = people_df.select("file_name", "player_name", "player_id").withColumn('match_num',
                                                                                             f.regexp_extract(
                                                                                                 f.col('file_name'),
                                                                                                 '[0-9]+', 0)).select(
    "match_num", "player_id", "player_name")

match_palyer_registry.repartition(4).select("match_num", "player_id", "player_name").write.parquet("/Users/leenapatil/IdeaProjects/capstone-project-leena/data/dim_match_player")
