import csv
import mysql.connector
import uuid
import pymysql
from datetime import datetime
import base64

# Establish MySQL connection
mysql_connection = pymysql.connect(
    host='localhost',
    user='root',
    password='12345678',
    database='social_media'
)


# Create MySQL cursor
cursor = mysql_connection.cursor()


def load_data_from_csv1(file_name, table_name):
    with open(file_name, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            user1_id, user2_id, timestamp = row
            friend = {
                'user_id_1': uuid.UUID(user1_id).bytes,
                'user_id_2': uuid.UUID(user2_id).bytes,
                'timestamp': datetime.strptime(timestamp.split('.')[0], '%Y-%m-%d %H:%M:%S')
            }
            columns = ', '.join(friend)
            placeholders = ', '.join(['%s'] * len(friend))
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            with mysql_connection.cursor() as cursor:
                cursor.execute(query, tuple(row))
        print("Done with file loading")


def load_data_from_csv2(file_name, table_name):
    with open(file_name, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            message_id, sender_id, receiver_id, content, timestamp = row
            message = {
                'message_id': uuid.UUID(message_id).bytes,
                'sender_user_id': uuid.UUID(sender_id).bytes,
                'receiver_user_id': uuid.UUID(receiver_id).bytes,
                'content': content,
                'timestamp': datetime.strptime(timestamp.split('.')[0], '%Y-%m-%d %H:%M:%S')
            }
        #     user_id, username, full_name, email, password, profile_picture, bio = row
        #     user = {
        #         'user_id': uuid.UUID(user_id).bytes,
        #         'username': username,
        #         'full_name': full_name,
        #         'email': email,
        #         'password': password,
        #         'profile_picture': profile_picture,
        #         'bio': bio
        #     }
            columns = ', '.join(message)
            placeholders = ', '.join(['%s'] * len(message))
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            with mysql_connection.cursor() as cursor:
                cursor.execute(query, tuple(row))
        print("Done with file loading")


# Function to insert data into MySQL
# Function to insert data into MySQL
# def insert_data(table_name, data):
#     columns = ', '.join(data.keys())
#     placeholders = ', '.join(['%s'] * len(data))
#     query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
#     with mysql_connection.cursor() as cursor:
#         cursor.execute(query, tuple(data.values()))


# Function to load data from CSV file
# def load_data_from_csv(file_name):
#     data = []
#     with open(file_name, 'r', encoding='utf-8') as file:
#         reader = csv.reader(file)
#         next(reader)  # Skip header row
#         for row in reader:
#             data.append(row)
#         print("dobe with file loading")
#     return data

# Load data from CSV files
# users_data_750k =
# load_data_from_csv('../datasets/k_750/users_750k.csv', 'users')
# posts_data_750k = load_data_from_csv('../datasets/k_750/posts_750k.csv',"posts")
# messages_data_750k =
load_data_from_csv1('../datasets/k_750/friends_750k.csv', "connections")
# friends_data_750k = 
load_data_from_csv2('../datasets/k_750/messages_750k.csv', "messages")

# Insert data into MySQL tables

# Users Table
# for row in users_data_750k:
#     user_id, username, full_name, email, password, profile_picture, bio = row
#     user = {
#         'user_id': uuid.UUID(user_id).bytes,
#         'username': username,
#         'full_name': full_name,
#         'email': email,
#         'password': password,
#         'profile_picture': profile_picture,
#         'bio': bio
#     }
#     insert_data('users', user)

# Posts Table
# for row in posts_data_750k:
#     post_id, user_id, content, timestamp = row
#     post = {
#         'post_id': uuid.UUID(post_id).bytes,
#         'user_id': uuid.UUID(user_id).bytes,
#         'content': content,
#         'timestamp': datetime.strptime(timestamp.split('.')[0], '%Y-%m-%d %H:%M:%S')
#     }
#     insert_data('posts', post)

# # Messages Table
# for row in messages_data_750k:
#     message_id, sender_id, receiver_id, content, timestamp = row
#     message = {
#         'message_id': uuid.UUID(message_id).bytes,
#         'sender_user_id': uuid.UUID(sender_id).bytes,
#         'receiver_user_id': uuid.UUID(receiver_id).bytes,
#         'content': content,
#         'timestamp': datetime.strptime(timestamp.split('.')[0], '%Y-%m-%d %H:%M:%S')
#     }
#     insert_data('messages', message)

# # Connections Table
# for row in friends_data_750k:
#     user1_id, user2_id, timestamp = row
#     friend = {
#         'user_id_1': uuid.UUID(user1_id).bytes,
#         'user_id_2': uuid.UUID(user2_id).bytes,
#         'timestamp': datetime.strptime(timestamp.split('.')[0], '%Y-%m-%d %H:%M:%S')
#     }
#     insert_data('connections', friend)

# Commit the changes and close the MySQL connection
mysql_connection.commit()
mysql_connection.close()
