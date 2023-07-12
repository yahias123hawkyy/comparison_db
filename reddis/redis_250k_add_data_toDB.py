import csv
import uuid
import random
from datetime import datetime
import redis
import json

# Connect to Redis
redis_client = redis.Redis(host='127.0.0.1', port=6379, db=0)  # Replace host and port with your Redis server details

# Function to insert data into Redis
def insert_data(key, data):
    redis_client.set(key, json.dumps(data))

# Function to load data from CSV file
def load_data_from_csv(file_name):
    data = []
    with open(file_name, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            data.append(row)
    return data

# Load data from CSV files
users_data_250k = load_data_from_csv('../datasets/k_250/users_250k.csv')
posts_data_250k = load_data_from_csv('../datasets/k_250/posts_250k.csv')
messages_data_250k = load_data_from_csv('../datasets/k_250/messages_250k.csv')
friends_data_250k = load_data_from_csv('../datasets/k_250/friends_250k.csv')

# Map attributes to Redis keys

# Users Collection
for row in users_data_250k:
    user_id, username, full_name, email, password, profile_picture, bio = row
    user = {
        'user_id': user_id,
        'username': username,
        'full_name': full_name,
        'email': email,
        'password': password,
        'profile_picture': profile_picture,
        'bio': bio
    }
    insert_data(f'user:{user_id}', user)

# Posts Collection
for row in posts_data_250k:
    post_id, user_id, content, timestamp = row
    post = {
        'post_id': post_id,
        'user_id': user_id,
        'content': content,
        'timestamp': timestamp
    }
    insert_data(f'post:{post_id}', post)

# Messages Collection
for row in messages_data_250k:
    message_id, sender_id, receiver_id, content, timestamp = row
    message = {
        'message_id': message_id,
        'sender_id': sender_id,
        'receiver_id': receiver_id,
        'content': content,
        'timestamp': timestamp
    }
    insert_data(f'message:{message_id}', message)

# Friends Collection
for row in friends_data_250k:
    user1_id, user2_id, timestamp = row
    friend = {
        'user1_id': user1_id,
        'user2_id': user2_id,
        'timestamp': timestamp
    }
    insert_data(f'friend:{user1_id}:{user2_id}', friend)

# Close the Redis connection
redis_client.close()
