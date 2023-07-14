import csv
import mysql.connector
import uuid
from datetime import datetime

mysql_connection = mysql.connector.connect(
    host='localhost',
    user='root',
    password='12345678',
    database='social_media'
)

cursor = mysql_connection.cursor()

# Function to insert data into MySQL
def insert_data(table_name, data):
    columns = ', '.join(data.keys())
    placeholders = ', '.join(['%s'] * len(data))
    query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    cursor.execute(query, tuple(data.values()))

# Function to load data from CSV file
def load_data_from_csv(file_name):
    data = []
    with open(file_name, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  
        for row in reader:
            data.append(row)
    return data

# Load data from CSV files
users_data_1m = load_data_from_csv('../datasets/m_1/users_1m.csv')
posts_data_1m = load_data_from_csv('../datasets/m_1/posts_1m.csv')
messages_data_1m = load_data_from_csv('../datasets/m_1/messages_1m.csv')
friends_data_1m = load_data_from_csv('../datasets/m_1/friends_1m.csv')


# Users Table
for row in users_data_1m:
    user_id, username, full_name, email, password, profile_picture, bio = row
    user = {
        'user_id': uuid.UUID(user_id).bytes,
        'username': username,
        'full_name': full_name,
        'email': email,
        'password': password,
        'profile_picture': profile_picture,
        'bio': bio
    }
    insert_data('users', user)

# Posts Table
for row in posts_data_1m:
    post_id, user_id, content, timestamp = row
    post = {
        'post_id': uuid.UUID(post_id).bytes,
        'user_id': uuid.UUID(user_id).bytes,
        'content': content,
        'timestamp': datetime.strptime(timestamp.split('.')[0], '%Y-%m-%d %H:%M:%S')
    }
    insert_data('posts', post)

# Messages Table
for row in messages_data_1m:
    message_id, sender_id, receiver_id, content, timestamp = row
    message = {
        'message_id': uuid.UUID(message_id).bytes,
        'sender_user_id': uuid.UUID(sender_id).bytes,
        'receiver_user_id': uuid.UUID(receiver_id).bytes,
        'content': content,
        'timestamp': datetime.strptime(timestamp.split('.')[0], '%Y-%m-%d %H:%M:%S')
    }
    insert_data('messages', message)

# Connections Table
for row in friends_data_1m:
    user1_id, user2_id, timestamp = row
    friend = {
        'user_id_1': uuid.UUID(user1_id).bytes,
        'user_id_2': uuid.UUID(user2_id).bytes,
        'timestamp': datetime.strptime(timestamp.split('.')[0], '%Y-%m-%d %H:%M:%S')
    }
    insert_data('connections', friend)

mysql_connection.commit()
cursor.close()
mysql_connection.close()




























# SCHEMA
# CREATE DATABASE IF NOT EXISTS social_media;

# USE social_media;

# CREATE TABLE IF NOT EXISTS users (
#   user_id BINARY(16) PRIMARY KEY,
#   username VARCHAR(255),
#   full_name VARCHAR(255),
#   email VARCHAR(255),
#   password VARCHAR(255),
#   profile_picture VARCHAR(255),
#   bio TEXT
# );

# CREATE TABLE IF NOT EXISTS posts (
#   post_id BINARY(16),
#   user_id BINARY(16),
#   content TEXT,
#   timestamp TIMESTAMP,
#   post_count INT,
#   message_count INT,
#   connection_count INT,
#   PRIMARY KEY (post_id, user_id)
# );

# CREATE TABLE IF NOT EXISTS messages (
#   message_id BINARY(16) PRIMARY KEY,
#   sender_user_id BINARY(16),
#   receiver_user_id BINARY(16),
#   content TEXT,
#   timestamp TIMESTAMP
# );

# CREATE TABLE IF NOT EXISTS connections (
#   user_id_1 BINARY(16),
#   user_id_2 BINARY(16),
#   timestamp TIMESTAMP,
#   PRIMARY KEY (user_id_1, user_id_2)
# );
