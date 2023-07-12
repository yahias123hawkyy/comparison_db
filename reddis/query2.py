import redis
import statistics
import scipy.stats as stats
from datetime import datetime
import csv
import os

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0)

USER_PREFIX = 'user:'
POST_PREFIX = 'post:'
QUERY_NAME = '1m_users_Query2'  # Replace with the name of your query

num_experiments = 31
response_times = []

for i in range(num_experiments):
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

    # Fetch users with at least 3 posts
    users_with_at_least_3_posts = []
    for user_id in user_ids_posted_at_least_3:
        user = r.hgetall(USER_PREFIX + user_id)
        users_with_at_least_3_posts.append(user)

    end_time = datetime.now()
    response_time = (end_time - start_time).total_seconds() * 1000  # in milliseconds
    response_times.append(response_time)



# Calculate the mean value
mean_value = statistics.mean(response_times)

csv_file = 'response_times_1m.csv'
file_exists = os.path.isfile(csv_file)

with open(csv_file, 'a', newline='') as file:
    writer = csv.writer(file)

    if not file_exists:
        writer.writerow(['Query', 'Response Times'])

    writer.writerow([QUERY_NAME] + response_times)

confidence_interval = stats.t.interval(
    0.95, len(response_times) - 1, loc=mean_value, scale=stats.sem(response_times))

# Print the results
print(f"Mean Value: {mean_value} ms")
print(f"95% Confidence Interval: {confidence_interval}")
