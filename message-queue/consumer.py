from kafka import KafkaConsumer
import pandas as pd
import csv

# Kafka consumer configuration
bootstrap_servers = 'localhost:9092'
topic_name = 'transaction_topic'
consumer_group_id = '1'

# Create a Kafka consumer
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    group_id=consumer_group_id
)

# CSV file configuration
csv_filename = 'messages.csv'

# Consume messages from Kafka and save them to the CSV file
with open(csv_filename, 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    for message in consumer:
        value = message.value.decode('utf-8') if message.value else None
        input_string = value.replace('"', '')
        values = value.split(',')
        csv_writer.writerow(values)
        csvfile.flush()
