from neo4j import GraphDatabase
import statistics
import scipy.stats as stats
from datetime import datetime
import csv
import os

uri = 'bolt://localhost:7687'
user = 'neo4j' 
password = 'neo4jj' 
driver = GraphDatabase.driver(uri, auth=(user, password))

query = '''
    MATCH (u:User {username: 'bjames'})
    RETURN u
'''

# Perform the experiments
num_experiments = 31
response_times = []

with driver.session() as session:
    for i in range(num_experiments):
        start_time = datetime.now()
        result1 = list(session.run(query))
        print(result1)
        end_time = datetime.now()
        response_time = (end_time - start_time).total_seconds() * \
            1000  
        response_times.append(response_time)

# Calculate the mean value
mean_value = statistics.mean(response_times)

csv_file = 'response_times_750k.csv'
query_name = '750k_users_Query1'  # Replace with the name of your query

file_exists = os.path.isfile(csv_file)

# Open the CSV file in append mode
with open(csv_file, 'a', newline='') as file:
    writer = csv.writer(file)

    # Write the header row if the file doesn't exist
    if not file_exists:
        writer.writerow(['Query', 'Response Times'])

    # Write the query name and response times as a single row
    writer.writerow([query_name] + response_times)

# Calculate the 95% confidence interval
confidence_interval = stats.t.interval(
    0.95, len(response_times) - 1, loc=mean_value, scale=stats.sem(response_times))

# Print the results
print(f"Mean Value: {mean_value} ms")
print(f"95% Confidence Interval: {confidence_interval}")

# Close the Neo4j driver
driver.close()
