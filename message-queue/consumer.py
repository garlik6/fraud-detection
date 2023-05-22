from kafka import KafkaConsumer
from hdfs import InsecureClient
import csv

# Kafka consumer configuration
bootstrap_servers = 'localhost:9092'
topic_name = 'transaction_topic'
consumer_group_id = '1'

# HDFS configuration
hdfs_namenode = 'http://localhost:9870'
hdfs_user = 'your_hdfs_username'
hdfs_path = '/your/hdfs/path/file.csv'

# Create an HDFS client
client = InsecureClient(hdfs_namenode, user=hdfs_user)

# Create a Kafka consumer
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    group_id=consumer_group_id
)

# CSV file configuration
csv_filename = 'messages.csv'

# Consume messages from Kafka and save them to the hdfs file

with client.write(hdfs_path, encoding='utf-8') as writer:
    while True:
        for message in consumer:
            key = message.key.decode('utf-8') if message.key else None
            value = message.value.decode('utf-8') if message.value else None
            line = f"{key}\t{value}\n"
            writer.write(line)
