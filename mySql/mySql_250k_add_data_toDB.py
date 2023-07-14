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


# Create MySQL cursor
cursor = mysql_connection.cursor()

# Function to insert data into MySQL


def insert_data(table_name, data):
    # print("started")
    columns = ', '.join(data.keys())
    placeholders = ', '.join(['%s'] * len(data))
    query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    cursor.execute(query, tuple(data.values()))

# Function to load data from CSV file


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
users_data_250k = load_data_from_csv('../datasets/k_250/users_250k.csv')
# posts_data_250k = load_data_from_csv('../datasets/k_250/posts_250k.csv')
# messages_data_250k = load_data_from_csv('../datasets/k_250/messages_250k.csv')
# friends_data_250k = load_data_from_csv('../datasets/k_250/friends_250k.csv')

# Insert data into MySQL tables

# Users Table
for row in users_data_250k:
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
# for row in posts_data_250k:
#     post_id, user_id, content, timestamp = row
#     post = {
#         'post_id': uuid.UUID(post_id).bytes,
#         'user_id': uuid.UUID(user_id).bytes,
#         'content': content,
#         'timestamp': datetime.strptime(timestamp.split('.')[0], '%Y-%m-%d %H:%M:%S')
#     }
#     insert_data('posts', post)

# # Messages Table
# for row in messages_data_250k:
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
# for row in friends_data_250k:
#     user1_id, user2_id, timestamp = row
#     friend = {
#         'user_id_1': uuid.UUID(user1_id).bytes,
#         'user_id_2': uuid.UUID(user2_id).bytes,
#         'timestamp': datetime.strptime(timestamp.split('.')[0], '%Y-%m-%d %H:%M:%S')
#     }
#     insert_data('connections', friend)

# Commit the changes and close the MySQL connection
mysql_connection.commit()
cursor.close()
mysql_connection.close()


# mport csv
# import mysql.connector
# import time

# # Database connection details
# db_config = {
#     'user': 'root',
#     'password': '',
#     'host': 'localhost',
#     'database': 'coursemanagementsystem_250k'
# }


# # File paths of the datasets
# dataset_files = {
#     'instructors': 'C:\\Users\\yosep\\OneDrive\\Desktop\\DBproject\\Dataset\\250K datasets\\instructors_250000.csv',
#     'students': 'C:\\Users\\yosep\\OneDrive\\Desktop\\DBproject\\Dataset\\250K datasets\\students_250000.csv',
#     'courses': 'C:\\Users\\yosep\\OneDrive\\Desktop\\DBproject\\Dataset\\250K datasets\\courses_250000.csv',
#     'assignments': 'C:\\Users\\yosep\\OneDrive\\Desktop\\DBproject\\Dataset\\250K datasets\\assignments_250000.csv',
#     'grades': 'C:\\Users\\yosep\\OneDrive\\Desktop\\DBproject\\Dataset\\250K datasets\\grades_250000.csv'
# }

# # Table names in the database
# table_names = {
#     'instructors':'instructors',
#     'students': 'students',
#     'courses': 'courses',
#     'assignments': 'assignments',
#     'grades': 'grades'
# }

# def import_dataset(file_path, table_name):
#     # Establish database connection
#     connection = mysql.connector.connect(**db_config)
#     cursor = connection.cursor()

#     # Import data from the CSV file
#     with open(file_path, 'r') as csvfile:
#         reader = csv.DictReader(csvfile)
#         start_time = time.time()
#         for row in reader:
#             # Generate the INSERT query dynamically based on the row data
#             insert_query = f"INSERT INTO {table_name} ({', '.join(row.keys())}) VALUES ({', '.join(['%s']*len(row))})"
#             cursor.execute(insert_query, list(row.values()))

#     # Commit the changes and close the connection
#     connection.commit()
#     cursor.close()
#     connection.close()

#     # Calculate and print the execution time
#     execution_time = time.time() - start_time
#     print(f"Imported {table_name} dataset in {execution_time} seconds")

# # Import datasets into respective tables
# for table, file_path in dataset_files.items():
#     print(f"Importing {table} dataset...")
#     import_dataset(file_path, table_names[table])
