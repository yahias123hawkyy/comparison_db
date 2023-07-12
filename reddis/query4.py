import redis
import statistics
import scipy.stats as stats
from datetime import datetime
import csv
import os

r = redis.Redis(host='localhost', port=6379)

USER_PREFIX = 'user:'
POST_PREFIX = 'post:'
MESSAGE_PREFIX = 'message:'
QUERY_NAME = '1m_users_Query4' 

# Perform the experiments
num_experiments = 31
response_times = []

for i in range(num_experiments):


    start_time = datetime.now()

    # Find users with at least 3 posts
    user_ids_posted_at_least_3 = []
    keys = r.scan_iter(match=POST_PREFIX + '*')
    for key in keys:
        if r.type(key) == b'set':
            post_count = r.scard(key)
            if post_count >= 3:
                user_id = key.decode('utf-8').split(':')[1]
                user_ids_posted_at_least_3.append(user_id)

    # Find users who sent at least 5 private messages
    users_with_at_least_3_posts_and_5_messages = []
    keys = r.scan_iter(match=MESSAGE_PREFIX + '*')
    for key in keys:
        if r.type(key) == b'set':
            message_count = r.scard(key)
            if message_count >= 5:
                user_id = key.decode('utf-8').split(':')[1]
                users_with_at_least_3_posts_and_5_messages.append(user_id)

    # Fetch users who satisfy both conditions
    users = []
    for user_id in users_with_at_least_3_posts_and_5_messages:
        user = r.hgetall(USER_PREFIX + user_id)
        users.append(user)

    end_time = datetime.now()
    response_time = (end_time - start_time).total_seconds() * 1000
    response_times.append(response_time)


mean_value = statistics.mean(response_times)

csv_file = 'response_times_1m.csv'
file_exists = os.path.isfile(csv_file)

# Open the CSV file in append mode
with open(csv_file, 'a', newline='') as file:
    writer = csv.writer(file)

    # Write the header row if the file doesn't exist
    if not file_exists:
        writer.writerow(['Query', 'Response Times'])

    # Write the query name and response times as a single row
    writer.writerow([QUERY_NAME] + response_times)

# Calculate the 95% confidence interval
confidence_interval = stats.t.interval(
    0.95, len(response_times)-1, loc=mean_value, scale=stats.sem(response_times))

# Print the results
print(f"Mean Value: {mean_value} ms")
print(f"95% Confidence Interval: {confidence_interval}")
