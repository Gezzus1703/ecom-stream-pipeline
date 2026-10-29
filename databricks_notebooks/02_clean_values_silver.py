#Lib Import
from pyspark.sql.functions import *
from pyspark.sql.types import *

#Configuration
storage_path = (
    "abfss://ecommerce@ecommercestoragegz.dfs.core.windows.net"
)
bronze_path = f"{storage_path}/bronze"
silver_path = f"{storage_path}/silver"
checkpoint_path = f"{silver_path}/_checkpoint/ecommerce_orders"

df_bronze = (
    spark.readStream
    .format("delta")
    .load(bronze_path)
)

#Clean and enrich
df_clean = (
    df_bronze
    .withColumn("timestamp",to_timestamp("timestamp"))
    .withColumn("price",when(col("price").isNull(),0.0).otherwise(col("price")))
    .withColumn("quantity",when(col("quantity").isNull(),1).otherwise(col("quantity")))
    .withColumn("total_amount",round(col("price")*col("quantity"), 2))
    .dropDuplicates(["order_id"])
    .filter(col("country")=="USA")
    .filter(col("state").isNotNull())
)

#Write to Silver Layer
query_silver = (
    df_clean.writeStream
    .format("delta")
    .outputMode('append')
    .option("checkpointLocation",checkpoint_path)
    .trigger(availableNow=True)
    .start(silver_path)
)

query_silver.awaitTermination()