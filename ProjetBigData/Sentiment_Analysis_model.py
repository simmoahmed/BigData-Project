from transformers import pipeline
from pymongo import MongoClient

# Initialize sentiment analysis pipeline
sent_pipeline = pipeline("sentiment-analysis")

# Function to perform sentiment analysis and store result in MongoDB
def analyze_sentiment_and_store(tweet_text):
    # Perform sentiment analysis
    result = sent_pipeline(tweet_text)

    # Connect to MongoDB
    client = MongoClient("mongodb://localhost:27017/")  # Update with your MongoDB connection string
    db = client["sentiment_analysis_db"]  # Replace with your database name
    collection = db["tweets"]  # Replace with your collection name

    # Create a document to store sentiment analysis result
    sentiment_data = {
        "tweet": tweet_text,
        "sentiment_label": result[0]['label'],
        "sentiment_score": result[0]['score']
    }

    #Insert the sentiment analysis result into MongoDB
    collection.insert_one(sentiment_data)
    print("Sentiment analysis result : ", sentiment_data)

# Example usage:
tweet_text = "This is a great day!"
analyze_sentiment_and_store(tweet_text)