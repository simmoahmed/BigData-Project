from kafka import KafkaProducer
import json
import praw
import time
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from pymongo import MongoClient
from transformers import pipeline

# Initialize sentiment analysis pipeline
sent_pipeline = pipeline("sentiment-analysis")

# Download NLTK data (if not downloaded)
nltk.download('vader_lexicon')

# Set up your Reddit API credentials
reddit_client_id = 'g7tD8jb5uxaxsERDWMTWRw'
reddit_client_secret = '57mp2urI2lazy6MQb3ptp18i3aAevA'
reddit_user_agent = 'MALIKI Ayoub'
username="Dense-Special-2992"
password="Ayoubmalikisp1"

# Set up your Kafka producer
bootstrap_servers = 'localhost:9092'
topic = 'reddit_topic_gaza'
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
                         
# Initialize the Reddit API wrapper
reddit = praw.Reddit(
    client_id=reddit_client_id,
    client_secret=reddit_client_secret,
    user_agent=reddit_user_agent,
    username = username,
    password = password
)

# Initialize Sentiment Intensity Analyzer
sia = SentimentIntensityAnalyzer()

# MongoDB configuration
mongo_client = MongoClient('localhost', 27017)
db = mongo_client['reddit_sentiment_db']
collection = db['reddit_sentiment_collection']

class RedditStreamListener:
    def on_submission(self, submission):
        # This method is called whenever a new Reddit submission is received
        reddit_post = {
            'title': submission.title,
            'score': submission.score,
            'url': submission.url,
            'author': str(submission.author),
            'created_utc': submission.created_utc,
        }

        # Perform sentiment analysis on the submission title
        sentiment_label = sent_pipeline(submission.title)
        reddit_post['sentiment'] = sentiment_label[0]['label']

        # Send the original Reddit post data to Kafka
        producer.send(topic, value=reddit_post)
        print("Reddit post sent to Kafka")

    def on_error(self, status_code):
        print(f"Error with status code: {status_code}")
        return True

# Set up Reddit stream listener
reddit_stream_listener = RedditStreamListener()

subreddit_name = 'Gaza'
subreddit = reddit.subreddit(subreddit_name)

while True:
    for submission in subreddit.new(limit=None):
        reddit_stream_listener.on_submission(submission)

    # Wait for 60 seconds before checking for new submissions again
    time.sleep(60)

# Close connections when done
producer.close()
mongo_client.close()