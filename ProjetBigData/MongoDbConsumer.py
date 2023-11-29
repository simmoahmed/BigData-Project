from kafka import KafkaConsumer
import json
from pymongo import MongoClient

# Set up your Kafka consumer
bootstrap_servers = 'localhost:9092'
topic = 'data_preprocessed'
consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Set up MongoDB connection
mongo_client = MongoClient('localhost', 27017)
db = mongo_client['reddit_db']
collection = db['reddit_posts_proc']

# Consume and store Reddit posts in MongoDB
for message in consumer:
    reddit_post = message.value
    collection.insert_one(reddit_post)
    print(f"Reddit post stored in MongoDB: {reddit_post}")

# Close the Kafka consumer when done
consumer.close()
