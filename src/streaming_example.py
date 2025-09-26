

from pyspark.sql.functions import current_timestamp, col
# Define file path and checkpoint path
file_path = "/databricks-datasets/structured-streaming/events"
checkpoint_path = "/tmp/ss-tutorial/_checkpoint"

# Clear previous checkpoint data if it exists (for fresh runs)
dbutils.fs.rm(checkpoint_path, True)


raw_df = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", checkpoint_path) # Enables schema inference and evolution
  .load(file_path)
)



processed_df = raw_df.withColumn("processing_time", current_timestamp()) \
                     .withColumn("event_type_upper", col("event_type").cast("string").alias("event_type").upper())


query = (processed_df.writeStream
  .format("delta")
  .outputMode("append") # Appends new rows to the Delta table
  .option("checkpointLocation", checkpoint_path + "/delta_checkpoint")
  .trigger(processingTime='10 seconds') # Triggers a micro-batch every 10 seconds
  .toTable("sales_review.sales_table.streaming_events_table") # Writes to a Delta table named "streaming_events_table"
)

# You can also use .start() and then .awaitTermination() for long-running streams
# query.awaitTermination()