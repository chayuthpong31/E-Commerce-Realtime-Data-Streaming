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
bronze_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze"
silver_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/silver"
checkpoint_path = silver_path + "/_checkpoint"

# Read Stream from bronze
df_raw = (
    spark.readStream
    .format("delta")
    .load(bronze_path)
    )

# Transformation
df_silver = (
    df_raw.withColumn("order_timestamp", to_timestamp(col("order_timestamp")))
    .fillna(0, subset=["quantity"])
    .withColumn("total_amount", col("product_price") * col("quantity"))
    .filter(col("country") == "USA")
)

# Write into Silver Layer
(
    df_silver.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .start(silver_path)
)

print(f"Streaming Started... Writing data to {silver_path}")