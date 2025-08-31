# Databricks notebook source
# MAGIC %pip install textblob
# MAGIC !python -m textblob.download_corpora

# COMMAND ----------

import pandas as pd
from textblob import TextBlob
from pyspark.sql.functions import col

# Load the table into a DataFrame
data = spark.table("workspace.default.media_customer_reviews")

# Convert Spark DataFrame to Pandas DataFrame
df = data.toPandas()

# Function to get sentiment polarity
def get_sentiment(text):
    analysis = TextBlob(text)
    return analysis.sentiment.polarity  # Polarity ranges from -1 (negative) to 1 (positive)

# Apply the function to the 'review_text' column
df['sentiment_polarity'] = df['review'].apply(get_sentiment)

# Classify reviews as 'negative' based on polarity
# A common threshold for negative sentiment is a polarity score less than 0
df['sentiment_category'] = df['sentiment_polarity'].apply(
    lambda score: 'negative' if score < 0 else ('positive' if score > 0 else 'neutral')
)

# Filter for negative reviews
negative_reviews = df[df['sentiment_category'] == 'negative']

print("Original DataFrame with Sentiment Scores:")
print(df)
print("\nNegative Reviews:")
print(negative_reviews)

# COMMAND ----------

# Convert Pandas DataFrame back to Spark DataFrame
negative_reviews_spark = spark.createDataFrame(negative_reviews)

# Create a table using the Spark DataFrame
negative_reviews_spark.write.format("delta").mode("overwrite").saveAsTable("default.negative_reviews")