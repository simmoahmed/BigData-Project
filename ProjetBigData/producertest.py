from kafka import KafkaProducer

# Define the Kafka server and topic to produce messages to
bootstrap_servers = ['localhost:9092']
topic = 'testTopic'

# Create a KafkaProducer instance
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Send a message to the specified topic
# Send a key-value message to the specified topic
key = b"key1"
value = b"Hello, this is a key-value message from the producer!"
producer.send(topic, key=key, value=value)


# Close the producer when done
producer.close()
