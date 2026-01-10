from pyspark.sql.types import *
from pyspark.sql.functions import *
import os
from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

# Define Schema
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("product_price", FloatType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("delivery_status", StringType(), True),
    StructField("order_timestamp", StringType(), True)
])

# Connection Configuration
namespace_name = os.getenv("NAME_SPACE")
event_hub_name = os.getenv("EVENT_HUB_NAME")
connection_string = os.getenv("CONNECTION_STR") + f";EntityPath={event_hub_name}"

# Kafka Config for Event Hubs
eh_conf = {
  "eventhubs.connectionString":
    spark._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string),
  "eventhubs.consumerGroup": "$Default",
  "startingPositions": """{"offset": "-1"}"""
}

# Read data from Event Hubs
df_raw = spark.readStream \
    .format("eventhubs") \
    .options(**eh_conf) \
    .load()

# Parse JSON
df = df_raw.selectExpr("CAST(body AS STRING) AS json") \
            .select(from_json("json", schema).alias("data")) \
            .select("data.*") 

# Azure Portal (Storage Account -> Access Keys)
storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME")
storage_account_key = os.getenv("STORAGE_ACCOUNT_KEY")

# Storage Configuration
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", 
    storage_account_key
)
container_name = os.getenv("CONTAINER_NAME") 

# Define Path
bronze_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze"
checkpoint_path = bronze_path + "/_checkpoint"

# Write Stream into Storage
df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .start(bronze_path)

print(f"Streaming started... Writing data to {bronze_path}")
