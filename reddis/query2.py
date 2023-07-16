import redis
import statistics
import scipy.stats as stats
from datetime import datetime
import csv
import os

r = redis.Redis(host='localhost', port=6379, db=0)

USER_PREFIX = 'user:'
POST_PREFIX = 'post:'
QUERY_NAME = '1m_users_Query2'  

num_experiments = 31
response_times = []





def get_user(user_id):
    user_key = USER_PREFIX + user_id
    user_data = redis_client.hgetall(user_key)
    if user_data:
        return json.loads(user_data)
    return None



for i in range(num_experiments):
    start_time = datetime.now()

  users_with_at_least_3_posts = []
keys = redis_client.scan_iter(match=POST_PREFIX + '*')
for key in keys:
    post_count = redis_client.scard(key)
    if post_count >= 3:
        user_id = key.decode('utf-8').split(':')[1]
        user = get_user(user_id)
        if user:
            users_with_at_least_3_posts.append(user)

    end_time = datetime.now()
    response_time = (end_time - start_time).total_seconds() * 1000  # in milliseconds
    response_times.append(response_time)



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
