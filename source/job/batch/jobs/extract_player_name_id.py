from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType
from pyspark.sql.functions import explode, col, map_entries

# Sample JSON data (replace with actual JSON data)
json_data = '''
{
  "info": {
    "balls_per_over": 6,
    "city": "Al Amarat",
    "dates": ["2022-08-22"],
    "event": {"name": "Men's T20 Asia Cup Qualifier", "match_number": 3},
    "gender": "male",
    "match_type": "T20",
    "match_type_number": 1742,
    "officials": {
      "match_referees": ["Sarika Prasad"],
      "reserve_umpires": ["Harikrishna Pillai"],
      "tv_umpires": ["BB Pradhan"],
      "umpires": ["Naveen D'Souza", "Tabarak Dar"]
    },
    "outcome": {"winner": "United Arab Emirates", "by": {"runs": 47}},
    "overs": 20,
    "player_of_match": ["KP Meiyappan"],
    "players": {
      "United Arab Emirates": ["Waseem Muhammad", "Chirag Suri", "V Aravind"],
      "Singapore": ["Aritra Dutta", "S Chandramohan", "R Gaznavi"]
    },
    "registry": {
      "people": {
        "A Bhargava": "192f2caf",
        "AR Puri": "1f490b28",
        "Aman Desai": "c10fd8f9"
      }
    },
    "season": "2022",
    "team_type": "international",
    "teams": ["United Arab Emirates", "Singapore"],
    "toss": {"decision": "field", "winner": "Singapore"},
    "venue": "Al Amerat Cricket Ground Oman Cricket (Ministry Turf 1)"
  }
}
'''

# Define SparkSession
spark = SparkSession.builder.appName("ReadPeopleField").getOrCreate()

# Define schema for the JSON data
schema = StructType([
    StructField("info", StructType([
        StructField("balls_per_over", IntegerType()),
        StructField("city", StringType()),
        StructField("dates", ArrayType(StringType())),
        StructField("event", StructType([
            StructField("name", StringType()),
            StructField("match_number", IntegerType())
        ])),
        StructField("gender", StringType()),
        StructField("match_type", StringType()),
        StructField("match_type_number", IntegerType()),
        StructField("officials", StructType([
            StructField("match_referees", ArrayType(StringType())),
            StructField("reserve_umpires", ArrayType(StringType())),
            StructField("tv_umpires", ArrayType(StringType())),
            StructField("umpires", ArrayType(StringType()))
        ])),
        StructField("outcome", StructType([
            StructField("winner", StringType()),
            StructField("by", StructType([
                StructField("runs", IntegerType())
            ]))
        ])),
        StructField("overs", IntegerType()),
        StructField("player_of_match", ArrayType(StringType())),
        StructField("players", MapType(StringType(), ArrayType(StringType()))),
        StructField("registry", StructType([
            StructField("people", MapType(StringType(), StringType()))
        ])),
        StructField("season", StringType()),
        StructField("team_type", StringType()),
        StructField("teams", ArrayType(StringType())),
        StructField("toss", StructType([
            StructField("decision", StringType()),
            StructField("winner", StringType())
        ])),
        StructField("venue", StringType())
    ]))
])

# Create DataFrame
df = spark.read.schema(schema).json(spark.sparkContext.parallelize([json_data]))

# Flatten the structure
df_flattened = df.selectExpr("info.*")

# Explode the people map into separate rows and extract key-value pairs into columns
df_people = df_flattened.withColumn("player_info", explode(map_entries(col("registry.people")))) \
    .selectExpr("*", "player_info.key as player_name", "player_info.value as player_id") \
    .drop("registry")

# Show the DataFrame
df_people.select("player_name").show(truncate=False)
