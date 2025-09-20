# Databricks notebook source
# MAGIC %pip install textblob
# MAGIC !python -m textblob.download_corpora

# COMMAND ----------
# can you see this change? on today?
import pandas as pd
from textblob import TextBlob
from pyspark.sql.functions import col

# Load the table into a DataFrame
data = spark.table("samples.bakehouse.media_customer_reviews")

from pyspark.sql import functions as F, Window

df = spark.table("samples.bakehouse.sales_transactions")

window_spec = Window.partitionBy("franchiseID").orderBy(F.desc("total_sales"))

result = (
    df.groupBy("franchiseID", "product")
      .agg(F.sum("quantity").alias("total_sales"))
      .withColumn("rn", F.row_number().over(window_spec))
      .filter(F.col("rn") == 1)
      .select("franchiseID", "product", "total_sales")
)


result.write.format("delta").mode("overwrite").saveAsTable("sales_review.sales_table.top_sales")