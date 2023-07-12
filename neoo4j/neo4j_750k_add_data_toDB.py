from neo4j import GraphDatabase
import csv
import uuid
from datetime import datetime

# Connect to Neo4j
# Replace 'bolt://localhost:7687' with your Neo4j bolt URL
driver = GraphDatabase.driver('bolt://localhost:7687', auth=("neo4j", "password"))

# Function to insert data into Neo4j
def insert_data(query, params):
    with driver.session() as session:
        session.run(query, **params)


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

# Insert data into Neo4j

# Users Table
for row in users_data_750k:
    user_id, username, full_name, email, password, profile_picture, bio = row
    params = {
        'user_id': uuid.UUID(user_id),
        'username': username,
        'full_name': full_name,
        'email': email,
        'password': password,
        'profile_picture': profile_picture,
        'bio': bio
    }
    query = '''
        CREATE (u:User {user_id: $user_id, username: $username, full_name: $full_name, email: $email,
        password: $password, profile_picture: $profile_picture, bio: $bio})
    '''
    insert_data(query, params)

# Posts Table
for row in posts_data_750k:
    post_id, user_id, content, timestamp = row
    params = {
        'post_id': uuid.UUID(post_id),
        'user_id': uuid.UUID(user_id),
        'content': content,
        'timestamp': datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
    }
    query = '''
        MATCH (u:User {user_id: $user_id})
        CREATE (p:Post {post_id: $post_id, content: $content, timestamp: $timestamp})
        CREATE (u)-[:POSTED]->(p)
    '''
    insert_data(query, params)

# Messages Table
for row in messages_data_750k:
    message_id, sender_id, receiver_id, content, timestamp = row
    params = {
        'message_id': uuid.UUID(message_id),
        'sender_id': uuid.UUID(sender_id),
        'receiver_id': uuid.UUID(receiver_id),
        'content': content,
        'timestamp': datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
    }
    query = '''
        MATCH (sender:User {user_id: $sender_id})
        MATCH (receiver:User {user_id: $receiver_id})
        CREATE (sender)-[:SENT]->(m:Message {message_id: $message_id, content: $content, timestamp: $timestamp})
        CREATE (m)-[:TO]->(receiver)
    '''
    insert_data(query, params)

# Friends Table
for row in friends_data_750k:
    user1_id, user2_id, timestamp = row
    params = {
        'user1_id': uuid.UUID(user1_id),
        'user2_id': uuid.UUID(user2_id),
        'timestamp': datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
    }
    query = '''
        MATCH (u1:User {user_id: $user1_id})
        MATCH (u2:User {user_id: $user2_id})
        CREATE (u1)-[:FRIENDS {timestamp: $timestamp}]->(u2)
    '''
    insert_data(query, params)

# Close the Neo4j driver
driver.close()
