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
    post_query = "SELECT user_id FROM posts"
    post_result = session.execute(post_query)
    post_counts = {}
    for row in post_result:
        user_id = row.user_id
        if user_id in post_counts:
            post_counts[user_id] += 1
        else:
            post_counts[user_id] = 1

    # Query users who sent at least 5 private messages
    message_query = "SELECT sender_user_id FROM messages"
    message_result = session.execute(message_query)
    message_counts = {}
    for row in message_result:
        user_id = row.sender_user_id
        if user_id in message_counts:
            message_counts[user_id] += 1
        else:
            message_counts[user_id] = 1

    # Find users who meet the criteria
    user_ids = [user_id for user_id, post_count in post_counts.items()
                if post_count >= 3 and message_counts.get(user_id, 0) >= 5]

    end_time = datetime.now()

    response_time = (end_time - start_time).total_seconds() * \
        1000  # in milliseconds
    response_times.append(response_time)



mean_value = statistics.mean(response_times)

csv_file = 'response_times_1m.csv'
query_name = '1m_messages_Query3'  
file_exists = os.path.isfile(csv_file)

with open(csv_file, 'a', newline='') as file:
    writer = csv.writer(file)

    if not file_exists:
        writer.writerow(['Query', 'Response Times'])

    writer.writerow([query_name] + response_times)

confidence_interval = stats.t.interval(0.95, len(
    response_times)-1, loc=mean_value, scale=stats.sem(response_times))

print(f"Mean Value: {mean_value} ms")
print(f"95% Confidence Interval: {confidence_interval}")

cluster.shutdown()
