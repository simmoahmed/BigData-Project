import json
import requests
from kafka import KafkaProducer

# Apify setup
apify_actor_id = 'your_apify_actor_id'
apify_api_key = 'your_apify_api_key'
apify_url = f'https://api.apify.com/v2/actor-tasks/elmaadoudi~wardata/runs?token=apify_api_qtv3oZnwcl4s1kt5mkC0N0yeUcBaHG2FFk5d'
# Kafka setup
bootstrap_servers = 'localhost:9092'
topic = 'reddit_topic_gaza'
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Function to fetch data from Apify
def fetch_data_from_apify():
    response = requests.get(apify_url)
    if response.status_code == 200:
        return response.json()['data']
    else:
        print(f"Error fetching data from Apify: {response.status_code}")
        return None

# Function to send data to Kafka
def send_to_kafka(data):
    for record in data:
        producer.send(topic, value=record)
        producer.flush()

# Main execution
if __name__ == "__main__":
    data = fetch_data_from_apify()
    if data:
        send_to_kafka(data)
