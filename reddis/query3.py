import redis
import statistics
import scipy.stats as stats
from datetime import datetime
import csv
import os

# Connect to Redis
r = redis.Redis(host='localhost', port=6379)

# Define the Redis key prefixes for each entity
USER_PREFIX = 'user:'
POST_PREFIX = 'post:'
MESSAGE_PREFIX = 'message:'
QUERY_NAME = '1m_users_Query3'  # Replace with the name of your query

# Perform the experiments
num_experiments = 31
response_times = []

for i in range(num_experiments):
    # Measure the response time
    start_time = datetime.now()

    # Find users with at least 3 posts
    user_ids_posted_at_least_3 = []
    keys = r.scan_iter(match=POST_PREFIX + '*')
    for key in keys:
        key_type = r.type(key)
        if key_type == b'set':
            post_count = r.scard(key)
            if post_count >= 3:
                user_id = key.decode('utf-8').split(':')[1]
                user_ids_posted_at_least_3.append(user_id)

    # Find users who sent at least 5 private messages
    user_ids_sent_at_least_5_messages = []
    keys = r.scan_iter(match=MESSAGE_PREFIX + '*')
    for key in keys:
        key_type = r.type(key)
        if key_type == b'set':
            message_count = r.scard(key)
            if message_count >= 5:
                user_id = key.decode('utf-8').split(':')[1]
                user_ids_sent_at_least_5_messages.append(user_id)

    # Fetch users who satisfy both conditions
    users_with_at_least_3_posts_and_5_messages = list(
        set(user_ids_posted_at_least_3).intersection(user_ids_sent_at_least_5_messages))

    users = []
    for user_id in users_with_at_least_3_posts_and_5_messages:
        user = r.hgetall(USER_PREFIX + user_id)
        users.append(user)

    end_time = datetime.now()
    response_time = (end_time - start_time).total_seconds() * 1000  # in milliseconds
    response_times.append(response_time)

for i, time in enumerate(response_times):
    print(f"Query {i+1} response time: {time} ms")

mean_value = statistics.mean(response_times)

csv_file = 'response_times_1m.csv'
file_exists = os.path.isfile(csv_file)

with open(csv_file, 'a', newline='') as file:
    writer = csv.writer(file)

    if not file_exists:
        writer.writerow(['Query', 'Response Times'])

    writer.writerow([QUERY_NAME] + response_times)

confidence_interval = stats.t.interval(
    0.95, len(response_times)-1, loc=mean_value, scale=stats.sem(response_times))

# Print the results
print(f"Mean Value: {mean_value} ms")
print(f"95% Confidence Interval: {confidence_interval}")
