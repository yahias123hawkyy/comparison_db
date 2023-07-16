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
QUERY_NAME = '1m_users_Query3'  


num_experiments = 31
response_times = []

for i in range(num_experiments):
    start_time = datetime.now()



    user_ids = r.keys('user:*')

    users_with_at_least_5_messages = []
    for user_id in user_ids:
            user_id = user_id.decode().split(':')[1]

            message_keys = r.keys(f'message:*:sender_user_id {user_id}')
            message_count = len(message_keys)

            if message_count >= 5:
                users_with_at_least_5_messages.append(user_id)

    user_ids = r.keys('user:*')
    user_post_counts = {}
    


    users_with_at_least_3_posts=[]
    for user_id in user_ids:
        start_time = datetime.now()

        user_id = user_id.decode().split(':')[1]

        user_details = r.hgetall(f'user:{user_id}')

        post_keys = r.keys(f'post:*:user_id {user_id}')
        post_count = len(post_keys)

        if post_count >= 3:
           users_with_at_least_3_posts.append(user_id)


    users_with_at_least_3_posts_and_5_messages = list(
        set(users_with_at_least_3_posts).intersection(users_with_at_least_5_messages))

    users = []
    for user_id in users_with_at_least_3_posts_and_5_messages:
        user = r.hgetall(USER_PREFIX + user_id)
        users.append(user)

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
    0.95, len(response_times)-1, loc=mean_value, scale=stats.sem(response_times))

# Print the results
print(f"Mean Value: {mean_value} ms")
print(f"95% Confidence Interval: {confidence_interval}")
