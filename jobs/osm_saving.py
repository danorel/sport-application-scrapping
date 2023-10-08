import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType
from pyspark.sql.functions import from_json, col

user_schema = StructType(
    [
        StructField("id", IntegerType(), False),
        StructField("name", FloatType(), False),
        StructField("traces", FloatType(), False),
        StructField("createdAt", FloatType(), False),
    ]
)

gpx_schema = StructType(
    [
        StructField("id", IntegerType(), False),
        StructField("name", FloatType(), False),
    ]
)


def write_to_neo4j(df: pd.DataFrame):
    (
        df.write.format("org.neo4j.spark.DataSource")
        .config("url", "bolt://127.0.0.1:7687")
        .config("authentication.type", "basic")
        .config("authentication.basic.username", "neo4j")
        .config("authentication.basic.password", "password")
        .mode("Overwrite")
        .save()
    )
    pass


spark = (
    SparkSession.builder
    .appName("Neo4jStreaming")
    .getOrCreate()
)

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("subscribe", "scrap-data")
    .option("delimeter", ",")
    .option("startingOffsets", "earliest")
    .load()
)

df.writeStream.foreachBatch(write_to_neo4j).outputMode(
    "update"
).start().awaitTermination()
