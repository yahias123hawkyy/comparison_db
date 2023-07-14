from cassandra.cluster import Cluster
import csv
import uuid
import datetime




cluster = Cluster(['localhost'])
session = cluster.connect('social_media')



def insert_data(table_name, data):
    insert_query = f"INSERT INTO {table_name} ({', '.join(data.keys())}) VALUES ({', '.join(['%s'] * len(data))})"
    session.execute(insert_query, tuple(data.values()))


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

# Insert data into Cassandra tables

# Users Table
for row in users_data_250k:
    user_id, username, full_name, email, password, profile_picture, bio = row
    user = {
        'user_id': uuid.UUID(user_id),
        'username': username,
        'full_name': full_name,
        'email': email,
        'password': password,
        'profile_picture': profile_picture,
        'bio': bio
    }
    insert_data('users', user)

# Posts Table
for row in posts_data_250k:
    post_id, user_id, content, timestamp = row
    post = {
        'post_id': uuid.UUID(post_id),
        'user_id': uuid.UUID(user_id),
        'content': content,
        'timestamp': datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
    }
    insert_data('posts', post)

# Messages Table
for row in messages_data_250k:
    message_id, sender_id, receiver_id, content, timestamp = row
    message = {
        'message_id': uuid.UUID(message_id),
        'sender_id': uuid.UUID(sender_id),
        'receiver_id': uuid.UUID(receiver_id),
        'content': content,
        'timestamp': datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
    }
    insert_data('messages', message)

# Friends Table
for row in friends_data_250k:
    user1_id, user2_id, timestamp = row
    friend = {
        'user_id_1': uuid.UUID(user1_id),
        'user_id_2': uuid.UUID(user2_id),
        'timestamp': datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
    }
    insert_data('connections', friend)

# Close the Cassandra connection
cluster.shutdown()
