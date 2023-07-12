from neo4j import GraphDatabase

# Connect to Neo4j
driver = GraphDatabase.driver('bolt://  ', auth=("neo4j", "neo4jj"))

# Function to insert data into Neo4j
def insert_data(query):
    with driver.session() as session:
        session.run(query)

# Clear existing data
# clear_data_query = '''
# MATCH (n)
# DETACH DELETE n
# '''
# insert_data(clear_data_query)

# Load data using LOAD CSV

# Users Table
# users_csv_query = '''
# LOAD CSV WITH HEADERS FROM 'file:///users_1m.csv' AS row
# CREATE (u:User {user_id: row.user_id, username: row.username, full_name: row.full_name,
#                 email: row.email, password: row.password, profile_picture: row.profile_picture,
#                 bio: row.bio})
# '''
# insert_data(users_csv_query)

# Posts Table
posts_csv_query = '''
LOAD CSV WITH HEADERS FROM 'file:///posts_1m.csv' AS row
MATCH (u:User {user_id: row.user_id})
CREATE (p:Post {post_id: row.post_id, content: row.content, timestamp: row.timestamp})
CREATE (u)-[:POSTED]->(p)
'''
insert_data(posts_csv_query)

# Messages Table
# messages_csv_query = '''
# LOAD CSV WITH HEADERS FROM 'file:///messages_1m.csv' AS row
# MATCH (sender:User {user_id: row.sender_id})
# MATCH (receiver:User {user_id: row.receiver_id})
# CREATE (sender)-[:SENT]->(m:Message {message_id: row.message_id, content: row.content, timestamp: row.timestamp})
# CREATE (m)-[:TO]->(receiver)
# '''
# insert_data(messages_csv_query)

# # Connections Table
# connections_csv_query = '''
# LOAD CSV WITH HEADERS FROM 'file:///friends_1m.csv' AS row
# MATCH (u1:User {user_id: row.user1_id})
# MATCH (u2:User {user_id: row.user2_id})
# CREATE (u1)-[:FRIENDS {timestamp: row.timestamp}]->(u2)
# '''
# insert_data(connections_csv_query)

# Close the Neo4j driver
driver.close()
