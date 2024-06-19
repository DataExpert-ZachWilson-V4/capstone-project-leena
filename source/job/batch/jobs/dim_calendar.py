from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType
from pyspark.sql.functions import explode, col, map_entries
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder.appName("calendar").getOrCreate()

# Read CSV file into DataFrame
df = spark.read.csv("/Users/leenapatil/IdeaProjects/capstone-project-leena/data/day_d_2021.csv", header=True, inferSchema=True)


date_df = df.selectExpr("dy_nat_db_clnd_dt as calendar_date", "dy_clnd_wk_num_in_mth as calendar_Week_Number_in_Month")
date_df.show()
# Create a temporary view
df.createOrReplaceTempView("my_temp_table")

# Query the temporary table
result = spark.sql("SELECT * FROM my_temp_table limit 60")
