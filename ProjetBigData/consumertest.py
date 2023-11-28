from kafka import KafkaConsumer

# Define the Kafka server and topic to subscribe to
bootstrap_servers = ['localhost:9092']
topic = 'testTopic'

# Create a KafkaConsumer instance
consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers)

# Start consuming messages
for message in consumer:
    # The message value is in the 'value' attribute
    print(f"Received message: {message.value.decode('utf-8')}")

# Close the consumer when done
consumer.close()
