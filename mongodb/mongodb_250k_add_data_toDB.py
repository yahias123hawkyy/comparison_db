import csv
from datetime import datetime
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['social_network_db']  

def insert_data(collection_name, data):
    collection = db[collection_name]
    collection.insert_many(data)

def load_data_from_csv(file_name):
    data = []
    with open(file_name, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            data.append(row)
    return data

users_data_250k = load_data_from_csv('../datasets/k_250/users_250k.csv')
posts_data_250k = load_data_from_csv('../datasets/k_250/posts_250k.csv')
messages_data_250k = load_data_from_csv('../datasets/k_250/messages_250k.csv')
friends_data_250k = load_data_from_csv('../datasets/k_250/friends_250k.csv')


# Users Collection
users_collection = []
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
    users_collection.append(user)

# Posts Collection
posts_collection = []
for row in posts_data_250k:
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
for row in messages_data_250k:
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
for row in friends_data_250k:
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

client.close()
