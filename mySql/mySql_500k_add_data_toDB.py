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



def insert_data(table_name, data):
    columns = ', '.join(data.keys())
    placeholders = ', '.join(['%s'] * len(data))
    query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    cursor.execute(query, tuple(data.values()))



def load_data_from_csv(file_name):
    data = []
    with open(file_name, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header row
        for row in reader:
            data.append(row)
        print("done with load data")
    return data


# Load data from CSV files
users_data_500k = load_data_from_csv('../datasets/k_500/users_500k.csv')
posts_data_500k = load_data_from_csv('../datasets/k_500/posts_500k.csv')
messages_data_500k = load_data_from_csv('../datasets/k_500/messages_500k.csv')
friends_data_500k = load_data_from_csv('../datasets/k_500/friends_500k.csv')

# Insert data into MySQL tables

# Users Table
for row in users_data_500k:
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
for row in posts_data_500k:
    post_id, user_id, content, timestamp = row
    post = {
        'post_id': uuid.UUID(post_id).bytes,
        'user_id': uuid.UUID(user_id).bytes,
        'content': content,
        'timestamp': datetime.strptime(timestamp.split('.')[0], '%Y-%m-%d %H:%M:%S')
    }
    insert_data('posts', post)

# # Messages Table
for row in messages_data_500k:
    message_id, sender_id, receiver_id, content, timestamp = row
    message = {
        'message_id': uuid.UUID(message_id).bytes,
        'sender_user_id': uuid.UUID(sender_id).bytes,
        'receiver_user_id': uuid.UUID(receiver_id).bytes,
        'content': content,
        'timestamp': datetime.strptime(timestamp.split('.')[0], '%Y-%m-%d %H:%M:%S')
    }
    insert_data('messages', message)

# # Connections Table
for row in friends_data_500k:
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


