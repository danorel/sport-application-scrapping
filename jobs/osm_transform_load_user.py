from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import from_json, col

from constants.kafka import READY_TO_TRANSFORM_LOAD_USER_TOPIC
from utils.logging import logger


def create_spark_session():
    """
    Creates the Spark Session with suitable configs.
    """
    try:
        # Spark session is established with cassandra and kafka jars. Suitable versions can be found in Maven repository.
        spark = SparkSession \
            .builder \
            .appName("SportStreaming") \
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.0.0,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
            .config("spark.cassandra.connection.host", "127.0.0.1") \
            .config("spark.cassandra.connection.port", "9042")\
            .config("spark.cassandra.auth.username", "cassandra") \
            .config("spark.cassandra.auth.password", "cassandra") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logger.info('Spark session created successfully')
    except Exception:
        logger.error("Couldn't create the spark session")

    return spark


def create_initial_dataframe(spark_session):
    """
    Reads the streaming data and creates the initial dataframe accordingly.
    """
    try:
        df = (
            spark_session
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "127.0.0.1:9092")
            .option("subscribe", READY_TO_TRANSFORM_LOAD_USER_TOPIC)
            .option("delimeter", ",")
            .option("startingOffsets", "earliest")
            .option("checkpointLocation", "/tmp/checkpoint")
            .load()
        )
        logger.info("Initial dataframe created successfully")
    except Exception as e:
        logger.warning(
            f"Initial dataframe couldn't be created due to exception: {e}")

    return df


def create_final_dataframe(df, spark_session):
    """
    Modifies the initial dataframe, and creates the final dataframe.
    """
    schema = StructType([
        StructField("id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("tracesCount", IntegerType(), nullable=True),
        StructField("createdAt", StringType(), nullable=True),
    ])

    df = (
        df
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )
    return df


def start_streaming(df):
    """
    Starts the streaming to table spark_streaming.random_names in cassandra
    """
    logger.info("Streaming is being started...")
    my_query = (
        df
        .writeStream
        .format("org.apache.spark.sql.cassandra")
        .outputMode("append")
        .options(table="user", keyspace="sport_streaming")
        .option("checkpointLocation", "/tmp/checkpoint")
        .start()
    )

    return my_query.awaitTermination()


spark = create_spark_session()
initial_df = create_initial_dataframe(spark)
final_df = create_final_dataframe(initial_df, spark)
start_streaming(final_df)
