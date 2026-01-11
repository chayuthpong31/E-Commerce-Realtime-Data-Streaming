from pyspark.sql.functions import *
import os
from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

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
gold_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/gold"
silver_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/silver"
checkpoint_path = gold_path + "/_checkpoint"

# Read Stream
df_silver = (
    spark.readStream
    .format("delta")
    .load(silver_path)
)

# Aggregation: Total Sales and Total Quantity per state per minute
df_gold = (
    df_silver
    .withWatermark("order_timestamp", "1 minute")
    .groupBy(
        window("order_timestamp", "1 minute"), 
        "state"
    )
    .agg(
        sum("total_amount").alias("total_sales"),
        sum("quantity").alias("total_quantity")
    )
    .select(
        col("window.start").alias("order_timestamp"),
        col("window.end").alias("order_timestamp_end"),
        "state",
        "total_sales",
        "total_quantity"
    )
)

# Write data into Gold Layer
(
    df_gold.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .start(gold_path)
)
