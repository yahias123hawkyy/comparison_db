import csv
import uuid
import random
from datetime import datetime
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
# Replace 'social_network_db' with your desired database name
db = client['social_network_db']

# Function to insert data into MongoDB collection


def insert_data(collection_name, data):
    collection = db[collection_name]
    collection.insert_many(data)


# Function to load data from CSV file


def load_data_from_csv(file_name):
    data = []
    with open(file_name, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            data.append(row)
    return data


# Load data from CSV files
users_data_750k = load_data_from_csv('../datasets/k_750/users_750k.csv')
posts_data_750k = load_data_from_csv('../datasets/k_750/posts_750k.csv')
messages_data_750k = load_data_from_csv('../datasets/k_750/messages_750k.csv')
friends_data_750k = load_data_from_csv('../datasets/k_750/friends_750k.csv')

# Map attributes to collections

# Users Collection
users_collection = []
for row in users_data_750k:
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
    users_collection.append(user)

# Posts Collection
posts_collection = []
for row in posts_data_750k:
    post_id, user_id, content, timestamp = row
    post = {
        'post_id': post_id,
        'user_id': user_id,
        'content': content,
        'timestamp': timestamp
    }
    posts_collection.append(post)

# Messages Collection
messages_collection = []
for row in messages_data_750k:
    message_id, sender_id, receiver_id, content, timestamp = row
    message = {
        'message_id': message_id,
        'sender_id': sender_id,
        'receiver_id': receiver_id,
        'content': content,
        'timestamp': timestamp
    }
    messages_collection.append(message)

# Friends Collection
friends_collection = []
for row in friends_data_750k:
    user1_id, user2_id, timestamp = row
    friend = {
        'user1_id': user1_id,
        'user2_id': user2_id,
        'timestamp': timestamp
    }
    friends_collection.append(friend)

# Insert data into MongoDB collections
insert_data('users', users_collection)
insert_data('posts', posts_collection)
insert_data('messages', messages_collection)
insert_data('friends', friends_collection)

# Close the MongoDB connection
client.close()
