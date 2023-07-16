import csv
import json
from datetime import datetime
import redis

redis_client = redis.Redis(host='127.0.0.1', port=6379, db=0)  

def insert_data(key, data):
    redis_client.set(key, json.dumps(data))

def load_data_from_csv(file_name):
    data = []
    with open(file_name, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            data.append(row)
    return data

# Load data from CSV files
users_data_1m = load_data_from_csv('../datasets/m_1/users_1m.csv')
posts_data_1m = load_data_from_csv('../datasets/m_1/posts_1m.csv')
messages_data_1m = load_data_from_csv('../datasets/m_1/messages_1m.csv')
friends_data_1m = load_data_from_csv('../datasets/m_1/friends_1m.csv')


# Users Collection
for row in users_data_1m:
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
for row in posts_data_1m:
    post_id, user_id, content, timestamp = row
    post = {
        'post_id': post_id,
        'user_id': user_id,
        'content': content,
        'timestamp': timestamp
    }
    insert_data(f'post:{post_id}', post)

# Messages Collection
for row in messages_data_1m:
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
for row in friends_data_1m:
    user1_id, user2_id, timestamp = row
    friend = {
        'user1_id': user1_id,
        'user2_id': user2_id,
        'timestamp': timestamp
    }
    insert_data(f'friend:{user1_id}:{user2_id}', friend)

# Close the Redis connection
redis_client.close()
