# Databricks notebook source 
%pip install textblob

from pyspark.sql.functions import current_timestamp, col, upper

file_path = "/databricks-datasets/structured-streaming/events"
checkpoint_path = "/Volumes/sales_review/sales_table/checkpoint"

raw_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .load(file_path)
)

processed_df = (
    raw_df.withColumn("processing_time", current_timestamp())
    .withColumn("event_type_upper", upper(col("action").cast("string")))
)

query = (
    processed_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path + "/delta_checkpoint")
    .trigger(availableNow=True)
    .toTable("sales_review.sales_table.new_streaming_events_table")
)