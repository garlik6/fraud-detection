from flask import Flask, request
from kafka import KafkaProducer

app = Flask(__name__)
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'transaction_topic'

def send_to_kafka(transaction):
    producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)
    producer.send(kafka_topic, transaction.encode('utf-8'))
    producer.flush()

@app.route('/transaction', methods=['POST'])
def transaction():
    transaction_data = f"{request.form.get('name')},{request.form.get('amount')},{request.form.get('remote_addr')}"
    send_to_kafka(transaction_data)
    return 'Transaction received and emitted to Kafka'
if __name__ == '__main__':
    app.run()
