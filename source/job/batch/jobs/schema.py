from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType, DateType, ArrayType


def get_schema():
    meta_type = StructType(
        [
            StructField("data_version", StringType(), True),
            StructField("created", DateType(), True),
            StructField("revision", IntegerType(), True)

        ]
    )
    info_type = StructType([
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
        StructField("players", MapType(StringType(), ArrayType(StringType()))),  # Corrected field type to MapType
        StructField("registry", StructType({
            StructField("people", MapType(StringType(), StringType()))
        })),
        StructField("season", StringType()),
        StructField("team_type", StringType()),
        StructField("teams", ArrayType(StringType())),
        StructField("toss", StructType({
            StructField("decision", StringType()),
            StructField("winner", StringType())
        })),
        StructField("venue", StringType())
    ])
    innings_type = ArrayType(
        StructType([
            StructField("team", StringType(), True),
            StructField("overs", ArrayType(StructType([
                StructField("over", IntegerType(), True),
                StructField("deliveries", ArrayType(StructType([
                    StructField("batter", StringType(), True),
                    StructField("bowler", StringType(), True),
                    StructField("non_striker", StringType(), True),
                    StructField("runs", StructType([
                        StructField("batter", IntegerType(), True),
                        StructField("extras", IntegerType(), True),
                        StructField("total", IntegerType(), True)
                    ]), True),
                    StructField("wickets", ArrayType(StructType([
                        StructField("player_out", StringType(), True),
                        StructField("fielders", ArrayType(StructType([
                            StructField("name", StringType(), True)
                        ])), True),
                        StructField("kind", StringType(), True)
                    ])), True)
                ])), True)
            ])), True)
        ]))
    schema = StructType(
        [
            StructField("meta", meta_type),
            StructField("info", info_type),
            StructField("innings", innings_type),
        ]
    )
    return schema
