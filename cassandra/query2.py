from cassandra.cluster import Cluster
from datetime import datetime
import csv
import os
import statistics
import scipy.stats as stats

cluster = Cluster(['localhost'])  
session = cluster.connect('social_media')  

query = "SELECT user_id, COUNT(*) as count FROM posts"

num_experiments = 31
response_times = []

for i in range(num_experiments):
    start_time = datetime.now()
    result = session.execute(query)
    user_ids_posted_at_least_3 = [row.user_id for row in result]
    end_time = datetime.now()
    
    response_time = (end_time - start_time).total_seconds() * 1000  # in milliseconds
    response_times.append(response_time)



# Calculate the mean value
mean_value = statistics.mean(response_times)

csv_file = 'response_times_1m.csv'
query_name = '1m_posts_Query2'  # Replace with the name of your query
file_exists = os.path.isfile(csv_file)

# Open the CSV file in append mode
with open(csv_file, 'a', newline='') as file:
    writer = csv.writer(file)

    if not file_exists:
        writer.writerow(['Query', 'Response Times'])

    writer.writerow([query_name] + response_times)

# Calculate the 95% confidence interval
confidence_interval = stats.t.interval(0.95, len(response_times)-1, loc=mean_value, scale=stats.sem(response_times))

# Print the results
print(f"Mean Value: {mean_value} ms")
print(f"95% Confidence Interval: {confidence_interval}")

# Close the Cassandra connection
cluster.shutdown()
