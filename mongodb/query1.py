import pymongo
import statistics
import scipy.stats as stats
from datetime import datetime
import csv
import os

client = pymongo.MongoClient('mongodb://localhost:27017')
db = client['social_network_db']

collection = db['users']

query = {'username': 'bjames'}

num_experiments = 31
response_times = []

for i in range(num_experiments):
    # Measure the response time
    start_time = datetime.now()
    result = collection.find(query)
    end_time = datetime.now()
    response_time = (end_time - start_time).total_seconds() * \
        1000  # in milliseconds
    response_times.append(response_time)

mean_value = statistics.mean(response_times)


csv_file = 'response_times_250k.csv'
query_name = '250k_Query1' 


file_exists = os.path.isfile(csv_file)

with open(csv_file, 'a', newline='') as file:
    writer = csv.writer(file)

    if not file_exists:
        writer.writerow(['Query', 'Response Times'])

    writer.writerow([query_name] + response_times)

    


confidence_interval = stats.t.interval(
    0.95, len(response_times)-1, loc=mean_value, scale=stats.sem(response_times))

print(f"Mean Value: {mean_value} ms")
print(f"95% Confidence Interval: {confidence_interval}")
