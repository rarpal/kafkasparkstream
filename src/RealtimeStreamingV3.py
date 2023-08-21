from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaStreamingExample") \
    .config('spark.logCont', 'true') \
    .getOrCreate()

spark.sparkContext.setLogLevel("OFF")

# Define the schema for the incoming JSON data
schema = StructType([
    StructField("key", StringType(), True),
    StructField("value", StringType(), True),
])

# Define the Kafka source
kafka_source = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "kafkaspark1") \
    .option("startingOffsets", "earliest") \
    .load()

# # Convert the value column from Kafka into a JSON structure
# parsed_data = kafka_source.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)") \
#     .select(from_json("value", schema).alias("data")) \
#     .select("data.*")

# # Display the streaming data
# query = parsed_data.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()

# # Wait for the streaming to finish
# query.awaitTermination()

kafka_source.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination()
