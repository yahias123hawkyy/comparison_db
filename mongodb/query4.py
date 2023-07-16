import pymongo
import statistics
import scipy.stats as stats
from datetime import datetime
import csv
import os

client = pymongo.MongoClient('mongodb://localhost:27017')
db = client['social_network_db']

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
    
    users_with_3_posts_and_5_private_messages = db.users.aggregate([
    {"$match": {"_id": {"$in": user_ids_with_3_posts}}},
    {"$lookup": {
        "from": "friends",
        "let": {"user_id": "$_id"},
        "pipeline": [
            {"$match": {
                "$expr": {
                    "$or": [
                        {"$eq": ["$$user_id", "$User ID of the first user"]},
                        {"$eq": ["$$user_id", "$User ID of the second user"]}
                    ]
                }
            }}
        ],
        "as": "connections"
    }},
    {"$unwind": "$connections"},  
    {"$match": {"connections": {"$exists": True}}},  
], allowDiskUse=True)

    
    end_time = datetime.now()
    
    response_time = (end_time - start_time).total_seconds() * 1000  # in milliseconds
    response_times.append(response_time)


mean_value = statistics.mean(response_times)

csv_file = 'response_times_250k.csv'
query_name = '250k_Query4'  
file_exists = os.path.isfile(csv_file)

with open(csv_file, 'a', newline='') as file:
    writer = csv.writer(file)

    if not file_exists:
        writer.writerow(['Query', 'Response Times'])

    writer.writerow([query_name] + response_times)

confidence_interval = stats.t.interval(0.95, len(response_times)-1, loc=mean_value, scale=stats.sem(response_times))

print(f"Mean Value: {mean_value} ms")
print(f"95% Confidence Interval: {confidence_interval}")
