# Databricks notebook source
# MAGIC %pip install textblob
# MAGIC !python -m textblob.download_corpora

# COMMAND ----------

# can you see this change? on today?
import pandas as pd#
from textblob import TextBlob
from pyspark.sql.functions import col
import nltk
from nltk.corpus import wordnet as wn

# Load the table into a DataFrame
data = spark.table("samples.bakehouse.media_customer_reviews")

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



# Ensure NLTK WordNet is downloaded
nltk.download('wordnet')

# Get a set of food-related words from WordNet using closure
def get_food_synonyms():
    food = wn.synset('food.n.02')
    food_synonyms = set()
    for s in food.closure(lambda s: s.hyponyms()):
        for w in s.lemma_names():
            food_synonyms.add(w.replace('_', ' ').lower())
    return food_synonyms

food_synonyms = get_food_synonyms()

# Load the table into a DataFrame
data = spark.table("samples.bakehouse.media_customer_reviews")
df = data.toPandas()

# Function to get sentiment polarity
def get_sentiment(text):
    analysis = TextBlob(text)
    return analysis.sentiment.polarity

# Function to extract food items mentioned in the review using WordNet food synonyms
def extract_foods_wordnet(text, food_set):
    text_lower = text.lower()
    return [food for food in food_set if food in text_lower]

# Apply sentiment analysis
df['sentiment_polarity'] = df['review'].apply(get_sentiment)
df['mentioned_foods'] = df['review'].apply(lambda x: extract_foods_wordnet(x, food_synonyms))

# Filter reviews that mention at least one food item
food_reviews = df[df['mentioned_foods'].apply(lambda x: len(x) > 0)]

# Get the review with the lowest sentiment polarity (most negative)
most_negative_review = food_reviews.loc[food_reviews['sentiment_polarity'].idxmin()]

# Display the most negative review and the food items mentioned
display(pd.DataFrame([most_negative_review]))

# COMMAND ----------

# Add 'mentioned_foods' column to df using the extract_foods_wordnet function and join as comma-separated string
df['mentioned_foods'] = df['review'].apply(lambda x: ', '.join(extract_foods_wordnet(x, food_synonyms)))

# Convert Pandas DataFrame back to Spark DataFrame
df_spark = spark.createDataFrame(df)

# Write the DataFrame to a Delta table
df_spark.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("sales_review.sales_table.negative_reviews")