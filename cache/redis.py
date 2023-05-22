import redis

redis_client = redis.Redis(host='localhost', port=6379, db=0)

# Retrieving a single value
value = redis_client.get('my_key')
print(value.decode('utf-8'))

# Retrieving hash data
hash_data = redis_client.hgetall('my_hash_key')
hash_data = {key.decode('utf-8'): value.decode('utf-8') for key, value in hash_data.items()}
print(hash_data)
