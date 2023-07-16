from cassandra.cluster import Cluster
from datetime import datetime
import csv
import os
import statistics
import scipy.stats as stats

cluster = Cluster(['localhost'])
session = cluster.connect('social_media')  

num_experiments = 31
response_times = []

for i in range(num_experiments):
    start_time = datetime.now()

    # Query users who posted at least 3 things
    post_query = "SELECT user_id, COUNT(*) AS post_count FROM posts"
    post_result = session.execute(post_query)
    post_user_ids = [row.user_id for row in post_result if row.post_count >= 3]

    # Query users who sent at least 5 private messages
    message_query = "SELECT sender_user_id, COUNT(*) AS message_count FROM messages"
    message_result = session.execute(message_query)
    message_user_ids = [row.sender_user_id for row in message_result if row.message_count >= 5]

    # Query connections
    connection_query = "SELECT user_id_1, user_id_2 FROM connections"
    connection_result = session.execute(connection_query)
    connection_user_ids = [(row.user_id_1, row.user_id_2) for row in connection_result]

    # Find common user_ids between the three sets
    user_ids = set(post_user_ids) & set(message_user_ids) & set(connection_user_ids)

    end_time = datetime.now()

    response_time = (end_time - start_time).total_seconds() * 1000  # in milliseconds
    response_times.append(response_time)



mean_value = statistics.mean(response_times)

csv_file = 'response_times_1m.csv'
query_name = '1m_connections_Query4'  
file_exists = os.path.isfile(csv_file)

with open(csv_file, 'a', newline='') as file:
    writer = csv.writer(file)

    if not file_exists:
        writer.writerow(['Query', 'Response Times'])

    writer.writerow([query_name] + response_times)

confidence_interval = stats.t.interval(0.95, len(response_times)-1, loc=mean_value, scale=stats.sem(response_times))

print(f"Mean Value: {mean_value} ms")
print(f"95% Confidence Interval: {confidence_interval}")

cluster.shutdown()
