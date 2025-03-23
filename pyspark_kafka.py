from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from datetime import datetime
from kafka_config import kafka_config

# Define your name for Kafka topics
MY_NAME = "Viacheslav"

# Create Spark Session
spark = SparkSession.builder \
    .appName("SensorDataAggregation") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .getOrCreate()

# Define schema for incoming JSON data
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True)
])

# Read alert conditions from CSV using the correct column names from your file
alerts_conditions = spark.read.csv("alerts_conditions.csv", header=True, inferSchema=True)
# Cache the small dataframe since we'll reuse it
alerts_conditions.cache()

# Read from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"VawEzo1ikLtrA8Ug8THa\";") \
    .option("subscribe", f"building_sensors_{MY_NAME}") \
    .load()

# Parse JSON data
parsed_df = df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select(
    col("data.sensor_id").alias("sensor_id"),
    col("data.timestamp").cast("timestamp").alias("timestamp"),
    col("data.temperature").alias("temperature"),
    col("data.humidity").alias("humidity")
)

# Apply watermark to handle late data (10 seconds)
df_with_watermark = parsed_df.withWatermark("timestamp", "10 seconds")

# Apply sliding window aggregation (1 minute window, sliding every 30 seconds)
windowed_aggregates = df_with_watermark \
    .groupBy(window("timestamp", "1 minute", "30 seconds")) \
    .agg(
    avg("temperature").alias("avg_temperature"),
    avg("humidity").alias("avg_humidity")
)


# Process each window and check against alert conditions
def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print("Empty batch, no data to process")
        return

    # Enrich the batch data with current timestamp for alerts
    batch_with_ts = batch_df.withColumn("current_timestamp", lit(datetime.now().isoformat(' ')))

    # Perform a cross join with alert conditions
    crossed = batch_with_ts.crossJoin(alerts_conditions)

    # Apply filtering to find matching alerts
    alerts_df = crossed.filter(
        # Temperature conditions
        ((col("temperature_min") != -999) & (col("avg_temperature") < col("temperature_min"))) |
        ((col("temperature_max") != -999) & (col("avg_temperature") > col("temperature_max"))) |
        # Humidity conditions
        ((col("humidity_min") != -999) & (col("avg_humidity") < col("humidity_min"))) |
        ((col("humidity_max") != -999) & (col("avg_humidity") > col("humidity_max")))
    )

    # Collect alerts to process them one by one
    alerts = alerts_df.collect()

    # Process each alert separately
    for alert in alerts:
        # Create a single alert dictionary
        alert_data = {
            "window": {
                "start": alert["window"].start.isoformat(),
                "end": alert["window"].end.isoformat()
            },
            "t_avg": float(alert["avg_temperature"]),
            "h_avg": float(alert["avg_humidity"]),
            "code": str(alert["code"]),
            "message": alert["message"],
            "timestamp": alert["current_timestamp"]
        }

        # Create a dataframe for this single alert
        single_alert_df = spark.createDataFrame([alert_data])

        # Convert to JSON format for Kafka
        kafka_df = single_alert_df.select(
            to_json(struct("*")).alias("value")
        )

        # Write this single alert to Kafka
        kafka_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
            .option("kafka.security.protocol", "SASL_PLAINTEXT") \
            .option("kafka.sasl.mechanism", "PLAIN") \
            .option("kafka.sasl.jaas.config",
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"VawEzo1ikLtrA8Ug8THa\";") \
            .option("topic", f"aggregated_alerts_{MY_NAME}") \
            .save()

        print(f"Alert generated: {alert_data}")


# Start stream processing
query = windowed_aggregates \
    .writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .start()

# Wait for termination
query.awaitTermination()