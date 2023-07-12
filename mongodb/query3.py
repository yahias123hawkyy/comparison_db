import pymongo
import statistics
import scipy.stats as stats
from datetime import datetime
import csv
import os

# Establish a connection to MongoDB
client = pymongo.MongoClient('mongodb://localhost:27017')
db = client['social_network_db']

# Choose a collection to query
collection = db['posts']

# Define your query
num_experiments = 31
response_times = []

for i in range(num_experiments):
    start_time = datetime.now()
    users_with_3_posts = db.posts.aggregate([
        {"$group": {"_id": "$user_id", "post_count": {"$sum": 1}}},
        {"$match": {"post_count": {"$gte": 3}}}
    ], allowDiskUse=True)
    
    user_ids_with_3_posts = [user["_id"] for user in users_with_3_posts]
    
    users_with_5_private_messages = db.messages.aggregate([
        {"$group": {"_id": "$Sender User ID", "message_count": {"$sum": 1}}},
        {"$match": {"message_count": {"$gte": 5}}}
    ], allowDiskUse=True)
    
    user_ids_with_5_private_messages = [user["_id"] for user in users_with_5_private_messages]
    
    users_with_3_posts_and_5_private_messages = db.users.find({
        "_id": {"$in": user_ids_with_3_posts},
        "_id": {"$in": user_ids_with_5_private_messages}
    })
    
    end_time = datetime.now()
    
    response_time = (end_time - start_time).total_seconds() * 1000  # in milliseconds
    response_times.append(response_time)

# Print the response times
for i, time in enumerate(response_times):
    print(f"Query {i+1} response time: {time} ms")

# Calculate the mean value
mean_value = statistics.mean(response_times)

csv_file = 'response_times_250k.csv'
query_name = '250k_Query3'  # Replace with the name of your query
file_exists = os.path.isfile(csv_file)

# Open the CSV file in append mode
with open(csv_file, 'a', newline='') as file:
    writer = csv.writer(file)

    # Write the header row if the file doesn't exist
    if not file_exists:
        writer.writerow(['Query', 'Response Times'])

    # Write the query name and response times as a single row
    writer.writerow([query_name] + response_times)

# Calculate the 95% confidence interval
confidence_interval = stats.t.interval(0.95, len(response_times)-1, loc=mean_value, scale=stats.sem(response_times))

# Print the results
print(f"Mean Value: {mean_value} ms")
print(f"95% Confidence Interval: {confidence_interval}")
