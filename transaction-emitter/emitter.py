from flask import Flask, request
from kafka import KafkaProducer
import redis
import json
import logging

app = Flask(__name__)
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'transaction_topic'
redis_host = 'localhost'
redis_port = 6379
redis_db = 0

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def send_to_kafka(transaction):
    producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)
    producer.send(kafka_topic, transaction.encode('utf-8'))
    producer.flush()

def check_transaction(transaction_data):
    user = transaction_data.split(",")[0]
    amount = float(transaction_data.split(",")[1])
    ip_address = transaction_data.split(",")[2]

    redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    user_data = redis_client.get(user)
    if user_data:
        user_data = json.loads(user_data.decode('utf-8'))
        average_amount = user_data['average']
        last_20_ips = user_data['last20Ips']
        if amount >= 2.0 * average_amount and ip_address not in last_20_ips:
            redis_client.close()
            return False
    redis_client.close()
    return True

@app.route('/transaction', methods=['POST'])
def transaction():
    transaction_data = f"{request.form.get('name')},{request.form.get('amount')},{request.form.get('remote_addr')}"

    if check_transaction(transaction_data):
        response = 'Transaction received and allowed'
        logging.info(f"Transaction allowed: {transaction_data}")
    else:
        response = 'Transaction received but not allowed'
        logging.warning(f"Transaction not allowed: {transaction_data}")

    send_to_kafka(transaction_data)
    return response

if __name__ == '__main__':
    app.run()
