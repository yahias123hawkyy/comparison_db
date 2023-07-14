from neo4j import GraphDatabase
import statistics
import scipy.stats as stats
from datetime import datetime
import csv
import os

uri = 'bolt://localhost:7687'  
user = 'neo4j'  
password = 'neo4jj'  #
driver = GraphDatabase.driver(uri, auth=(user, password))

query1 = '''
     MATCH (m:Message)
    WITH m.sender_user_id AS user_id, COUNT(*) AS mCount
    WHERE mCount >= 5
    RETURN user_id
'''
query2 = '''
MATCH (p:Post)
    WITH p.user_id AS user_id, COUNT(*) AS postCount
    WHERE postCount >= 3
    RETURN user_id
'''

num_experiments =31
response_times = []

with driver.session() as session:
    for i in range(num_experiments):
        # Measure the response time
        start_time = datetime.now()
        result1 = list(session.run(query1))
        result2=  list(session.run(query2))
        combined_users = set(result1).union(result2)

        end_time = datetime.now()
        response_time = (end_time - start_time).total_seconds() * \
            1000  # in milliseconds
        response_times.append(response_time)

mean_value = statistics.mean(response_times)

csv_file = 'response_times_750k.csv'
query_name = '750k_users_Query4'  # Replace with the name of your query

file_exists = os.path.isfile(csv_file)

with open(csv_file, 'a', newline='') as file:
    writer = csv.writer(file)

    if not file_exists:
        writer.writerow(['Query', 'Response Times'])

    writer.writerow([query_name] + response_times)

confidence_interval = stats.t.interval(
    0.95, len(response_times) - 1, loc=mean_value, scale=stats.sem(response_times))

print(f"Mean Value: {mean_value} ms")
print(f"95% Confidence Interval: {confidence_interval}")

driver.close()
