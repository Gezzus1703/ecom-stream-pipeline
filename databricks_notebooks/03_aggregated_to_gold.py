#Lib Import
from pyspark.sql.functions import *
from pyspark.sql.types import *

#Configuration
storage_path = "abfss://ecommerce@ecommercestoragegz.dfs.core.windows.net"
silver_path = f"{storage_path}/silver"
gold_path = f"{storage_path}/gold"
checkpoint_path = f"{gold_path}/_checkpoint/ecommerce_orders"

df_silver = (
    spark.readStream
    .format("delta")
    .load(silver_path)
)

#Aggregation: Total sales and total items sold per state per minute
df_gold = (
    df_silver
    .withWatermark("timestamp","1 minute")
    .groupBy(
        window("timestamp", "1 minute"),
        col("state"),
        col("category"),
        col("delivery_status")
    )
    .agg(
        round(sum("total_amount"), 2).alias("total_sales"),
        sum("quantity").alias("total_items")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("state"),
        col("category"),
        col("delivery_status"),
        col("total_sales"),
        col("total_items")
    )
)

#Write to Gold layer
query_gold = (
    df_gold
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start(gold_path)
)

query_gold.awaitTermination()