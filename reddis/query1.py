import redis
import statistics
import scipy.stats as stats
from datetime import datetime
import csv
import os

r = redis.Redis(host='localhost', port=6379,db=0)

# USER_PREFIX = 'user:'
QUERY_NAME = '1m_users_Query1'  

# username = 'bjames'
# query = f"{USER_PREFIX}{username}"

num_experiments = 31
response_times = []

def retrieve_users_by_username(rr, username):
    result = []
    user_keys = rr.keys('user:*')
    for key in user_keys:
        user = rr.hgetall(key)
        if user[b'username'].decode('utf-8') == username:
            result.append(user)
    return result



for i in range(num_experiments):
    start_time = datetime.now()
    resultt= retrieve_users_by_username(r, 'bjames')
    end_time = datetime.now()
    response_time = (end_time - start_time).total_seconds() * 1000  
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

print(f"Mean Value: {mean_value} ms")
print(f"95% Confidence Interval: {confidence_interval}")
