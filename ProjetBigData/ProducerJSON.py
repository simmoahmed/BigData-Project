from kafka import KafkaProducer
import json
import time

# Set up your Kafka producer
bootstrap_servers = 'localhost:9092'
topic = 'Extract_Data_Topic'
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,api_version=(0,11,5),
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

class RedditStreamListener:
    def on_submission(self, submission):
        # This method is called whenever a new Reddit submission is received
        producer.send(topic, value=submission)
        print("Reddit post sent to Kafka")

    def on_error(self, status_code):
        print(f"Error with status code: {status_code}")
        return True

# Set up Reddit stream listener
reddit_stream_listener = RedditStreamListener()

# Read data from a JSON file with explicit encoding (MacRoman)
json_file_path = 'D:\Apache Airflow\ProjetBigData\englishtweets.json'

with open(json_file_path, 'r', encoding='MacRoman') as json_file:
    reddit_data = json.load(json_file)


# Simulate Reddit submissions from the loaded data
for submission_data in reddit_data:
    reddit_stream_listener.on_submission(submission_data)

# Close the Kafka producer when done
producer.close()