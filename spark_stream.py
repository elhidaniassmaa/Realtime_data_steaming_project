import logging
import sys

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

logging.basicConfig(level=logging.INFO)

# Volume Docker persistant (spark_checkpoints) : survit à la recréation du conteneur
CHECKPOINT_PATH = "/opt/spark/checkpoints/created_users"


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    logging.info("Keyspace spark_streams ready")


def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            postcode TEXT,
            email TEXT,
            username TEXT,
            dob TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT
        )
    """)
    logging.info("Table created_users ready")


def create_cassandra_connection():
    try:
        cluster = Cluster(['cassandra'])
        return cluster.connect()
    except Exception as e:
        logging.error(f"Error creating Cassandra connection: {e}")
        return None


def create_spark_connection():
    try:
        spark = SparkSession.builder \
            .appName("SparkDataStreaming") \
            .config("spark.cassandra.connection.host", "cassandra") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logging.info("Spark session created")
        return spark
    except Exception as e:
        logging.error(f"Error creating Spark session: {e}")
        return None


def connect_to_kafka(spark):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "user_created") \
        .option("startingOffsets", "earliest") \
        .load()


def create_selection_df_from_kafka(kafka_df):
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("address", StringType(), True),
        StructField("postcode", StringType(), True),
        StructField("email", StringType(), True),
        StructField("username", StringType(), True),
        StructField("dob", StringType(), True),
        StructField("registered_date", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("picture", StringType(), True),
    ])
    return kafka_df.selectExpr("CAST(value AS STRING) AS value") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")


if __name__ == "__main__":
    session = create_cassandra_connection()
    spark = create_spark_connection()

    # Code de sortie 1 en cas d'échec : sans cela, `restart: on-failure`
    # ne relancerait pas le conteneur (un exit 0 est considéré comme un succès)
    if session is None or spark is None:
        logging.error("Initialisation impossible (Cassandra ou Spark), arrêt.")
        sys.exit(1)

    create_keyspace(session)
    create_table(session)

    selection_df = create_selection_df_from_kafka(connect_to_kafka(spark))

    query = selection_df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .option("keyspace", "spark_streams") \
        .option("table", "created_users") \
        .start()
    query.awaitTermination()