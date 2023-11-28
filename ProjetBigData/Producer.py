from kafka import KafkaProducer
import praw
import json

# Set up your Reddit API credentials
reddit_client_id = 'g7tD8jb5uxaxsERDWMTWRw'
reddit_client_secret = '57mp2urI2lazy6MQb3ptp18i3aAevA'
reddit_user_agent = 'MALIKI Ayoub'
username="Dense-Special-2992"
password="Ayoubmalikisp1"

# Set up your Kafka producer
bootstrap_servers = 'localhost:9092'
topic = 'reddit_topic'
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

# Example: Get the top 5 hot posts from the "python" subreddit and send to Kafka
subreddit_name = 'Palestine'
subreddit = reddit.subreddit(subreddit_name)
hot_posts = subreddit.hot(limit=5)

for post in hot_posts:
    post_data = {
        'title': post.title,
        'score': post.score,
        'url': post.url,
    }
    producer.send(topic, value=post_data)
    print("Reddit post sent to Kafka")

# Close the Kafka producer when done
producer.close()

