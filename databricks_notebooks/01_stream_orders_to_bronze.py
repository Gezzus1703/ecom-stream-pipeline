#Lib Import
from pyspark.sql.types import *
from pyspark.sql.functions import *

#Schema of dataset
schema = StructType([
    StructField("order_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("category", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", IntegerType()),
    StructField("total_amount", DoubleType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("country", StringType()),
    StructField("latitude", StringType()),
    StructField("longitude", StringType()),
    StructField("delivery_status", StringType()),
])

#Azure Event Hub Configuration
event_hub_namespace = "ecommerce-namespace.servicebus.windows.net"
event_hub_name = "ecommerce-orders"
service_credential_name = "eventhub_credential"

eh_config = {
    'kafka.bootstrap.servers': f"{event_hub_namespace}:9093",
    'subscribe': event_hub_name,
    'databricks.serviceCredential': service_credential_name
}

#Read Stream from Event Hub
df_raw = (
    spark.readStream
    .format("kafka")
    .options(**eh_config)
    .load()
)

# Parse JSON messages
df_orders = (
    df_raw
    .selectExpr("CAST(value AS STRING) AS json")
    .select(from_json(col("json"), schema).alias("data"))
    .select("data.*")
)

# ADLS Gen2 paths - Unity Catalog External Location
storage_path = (
    "abfss://ecommerce@ecommercestoragegz.dfs.core.windows.net"
)
bronze_path = f"{storage_path}/bronze"
checkpoint_path = f"{bronze_path}/_checkpoint/ecommerce_orders"

# Write stream to Delta Lake Bronze
query_bronze = (
    df_orders.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start(bronze_path)
)

query_bronze.awaitTermination()