from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

csv_schema = StructType([
    StructField("Datetime", TimestampType(), True),
    StructField("Temperature", DoubleType(), True),
    StructField("Humidity", DoubleType(), True),
    StructField("WindSpeed", DoubleType(), True),
    StructField("GeneralDiffuseFlows", DoubleType(), True),
    StructField("DiffuseFlows", DoubleType(), True),
    StructField("PowerConsumption_Zone1", DoubleType(), True),
    StructField("PowerConsumption_Zone2", DoubleType(), True),
    StructField("PowerConsumption_Zone3", DoubleType(), True),
])

spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:29092") \
    .option("subscribe", "testTopic") \
    .load()

df = df.selectExpr("CAST(value AS STRING) as csv_data") \
    .select(from_json("csv_data", csv_schema).alias("data"))

#Exemple of analysis 

df_analysis = df.select("data.*")  # Select all columns from the 'data' struct

# Calculate average temperature
average_temperature = df_analysis.select(avg("Temperature").alias("AverageTemperature")).collect()[0]["AverageTemperature"]
print(f"Average Temperature: {average_temperature}")

# Count the number of records
record_count = df_analysis.select(count("*").alias("RecordCount")).collect()[0]["RecordCount"]
print(f"Number of Records: {record_count}")

# Find the minimum and maximum values for some columns
min_max_values = df_analysis.select(
    min("Temperature").alias("MinTemperature"),
    max("Temperature").alias("MaxTemperature"),
    min("Humidity").alias("MinHumidity"),
    max("Humidity").alias("MaxHumidity"),
    min("WindSpeed").alias("MinWindSpeed"),
    max("WindSpeed").alias("MaxWindSpeed"),
).collect()[0]

print(f"Min Temperature: {min_max_values['MinTemperature']}")
print(f"Max Temperature: {min_max_values['MaxTemperature']}")
print(f"Min Humidity: {min_max_values['MinHumidity']}")
print(f"Max Humidity: {min_max_values['MaxHumidity']}")
print(f"Min Wind Speed: {min_max_values['MinWindSpeed']}")
print(f"Max Wind Speed: {min_max_values['MaxWindSpeed']}")


query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()