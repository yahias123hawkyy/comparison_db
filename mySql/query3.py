import mysql.connector
from datetime import datetime
import csv
import os
import statistics
import scipy.stats as stats

connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="12345678",
    database="social_media"
)  

cursor = connection.cursor()

num_experiments = 31
response_times = []

for i in range(num_experiments):
    start_time = datetime.now()

    # Query users who posted at least 3 things
    post_query = """
    SELECT user_id FROM posts GROUP BY user_id HAVING COUNT(*) >= 3
    """
    cursor.execute(post_query)
    post_result = cursor.fetchall()
    post_user_ids = [str(row[0]) for row in post_result]

    # Query users who sent at least 5 private messages
    message_query = """
    SELECT sender_user_id FROM messages GROUP BY sender_user_id HAVING COUNT(*) >= 5
    """
    cursor.execute(message_query)
    message_result = cursor.fetchall()
    message_user_ids = [str(row[0]) for row in message_result]

    # Find common user_ids between the two sets
    user_ids = set(post_user_ids) & set(message_user_ids)

    end_time = datetime.now()

    response_time = (end_time - start_time).total_seconds() * 1000  # in milliseconds
    response_times.append(response_time)

mean_value = statistics.mean(response_times)

csv_file = 'response_times_750k.csv'
query_name = '750k_Query3' 
file_exists = os.path.isfile(csv_file)

with open(csv_file, 'a', newline='') as file:
    writer = csv.writer(file)

    if not file_exists:
        writer.writerow(['Query', 'Response Times'])

    writer.writerow([query_name] + response_times)

confidence_interval = stats.t.interval(0.95, len(response_times)-1, loc=mean_value, scale=stats.sem(response_times))

print(f"Mean Value: {mean_value} ms")
print(f"95% Confidence Interval: {confidence_interval}")

cursor.close()
connection.close()
