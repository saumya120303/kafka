from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("S3ToSnowflake") \
    .getOrCreate()

# Define schema for the CSV data
schema = StructType([
    StructField("msisdn", StringType(), True),
    StructField("week_number", IntegerType(), True),
    StructField("revenue_usd", FloatType(), True)
    # StructField("timestamp", StringType(), True)  # Add timestamp field
])

# Define Kafka configurations
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "rev1_s3"

# Read data from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON data
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Snowflake configurations
sf_options = {
    "sfURL": "pixxhrz-sp20557.snowflakecomputing.com",
    "sfUser": "saumyamoc",
    "sfPassword": "Saumya1!",
    "sfDatabase": "trials3_db",
    "sfSchema": "public",
    "sfWarehouse": "trial_s3",
}

# Function to write the stream to Snowflake
def foreach_batch_function(df, epoch_id):
    df.write \
        .format("snowflake") \
        .options(**sf_options) \
        .option("dbtable", "trials3t") \
        .mode("append") \
        .save()

# Write the stream to Snowflake
query = parsed_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .outputMode("append") \
    .start()

query.awaitTermination()
