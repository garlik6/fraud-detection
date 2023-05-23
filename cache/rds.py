import random
import json
import redis

# Connect to Redis
redis_host = 'localhost'  # Redis server host
redis_port = 6379  # Redis server port
redis_db = 0  # Redis database number
redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

# Generate random data for multiple users
num_users = 10

for i in range(1, num_users + 1):
    user_data = {
        "average": random.randint(1000, 2000),
        "last20Ips": [
            f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}"
            for _ in range(20)
        ],
    }
    redis_client.set(f"user{i}", json.dumps(user_data))

# Close the Redis connection
redis_client.close()
