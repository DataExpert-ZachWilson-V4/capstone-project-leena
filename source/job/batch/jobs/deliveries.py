# This section is responisble for reading all jsons and saving df
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, input_file_name
from pyspark.sql.types import *


# Initialize Spark session
spark = SparkSession.builder.appName("JSONFlatten").getOrCreate()

# Load the JSON file into a DataFrame
df = spark.read.format('json').option("multiline", True).load("/Users/leenapatil/Documents/all_json/640*")
match_num_df = df.withColumn("input_file_name",input_file_name())
match_num_df.cache()

# Explode the 'overs' array

# Explode the 'deliveries' array within each 'over'
#df = df.withColumn("deliveries", explode(col("overs.deliveries")))

#df_new = df.select(explode(col("deliveries")))
#innings_df.count()
#innings_df.printSchema()
#df.show(truncate=False)

innings_df = match_num_df.withColumn("innings", explode(col("innings")))
innings_df.count()
innings_df.explain()

overs_df = innings_df.withColumn("overs", explode("innings.overs"))
overs_df.count()
overs_df.printSchema()


deliveries_df = overs_df.withColumn("deliveries", explode("overs.deliveries"))
deliveries_df.count()
deliveries_df.select("deliveries").printSchema()


data1_df = deliveries_df.select("innings.team","innings.declared","deliveries.batter","deliveries.bowler", "deliveries.runs.total","deliveries.wickets")
data1_df.registerTempTable("data1")
# use createOrReplaceTempView
spark.sql("select sum(total) as total, team from data1 group by team").show()