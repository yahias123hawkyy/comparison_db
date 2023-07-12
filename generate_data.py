from faker import Faker
import csv
import uuid
import random
from datetime import datetime


#   generate data for each collection/ table


def generate_users_data(num_records):
    fake = Faker()
    users_data = []
    for _ in range(num_records):
        user_id = uuid.uuid4()
        username = fake.user_name()
        full_name = fake.name()
        email = fake.email()
        password = fake.password()
        profile_picture = fake.image_url()
        bio = fake.text(max_nb_chars=200)
        users_data.append([user_id, username, full_name,
                          email, password, profile_picture, bio])
    return users_data


def generate_posts_data(num_records, users_data):
    fake = Faker()
    posts_data = []
    for _ in range(num_records):
        post_id = uuid.uuid4()
        user_id, _, _, _, _, _, _ = random.choice(users_data)
        content = fake.text(max_nb_chars=200)
        timestamp = datetime.now()
        posts_data.append([post_id, user_id, content, timestamp])
    return posts_data


def generate_messages_data(num_records, users_data):
    fake = Faker()
    messages_data = []
    for _ in range(num_records):
        message_id = uuid.uuid4()
        sender_id, _, _, _, _, _, _ = random.choice(users_data)
        receiver_id, _, _, _, _, _, _ = random.choice(users_data)
        content = fake.text(max_nb_chars=200)
        timestamp = datetime.now()
        messages_data.append(
            [message_id, sender_id, receiver_id, content, timestamp])
    return messages_data


def generate_friends_data(num_records, users_data):
    friends_data = []
    for _ in range(num_records):
        user1_id, _, _, _, _, _, _ = random.choice(users_data)
        user2_id, _, _, _, _, _, _ = random.choice(users_data)
        timestamp = datetime.now()
        friends_data.append([user1_id, user2_id, timestamp])
    return friends_data


# Generate data for 250,000 records
users_data_250k = generate_users_data(250000)
posts_data_250k = generate_posts_data(250000, users_data_250k)
messages_data_250k = generate_messages_data(250000, users_data_250k)
friends_data_250k = generate_friends_data(250000, users_data_250k)

# Generate data for 500,000 records

users_data_500k = generate_users_data(500000)
posts_data_500k = generate_posts_data(500000, users_data_500k)
messages_data_500k = generate_messages_data(500000, users_data_500k)
friends_data_500k = generate_friends_data(500000, users_data_500k)

# Generate data for 750,000 records

users_data_750k = generate_users_data(750000)
posts_data_750k = generate_posts_data(750000, users_data_750k)
messages_data_750k = generate_messages_data(750000, users_data_750k)
friends_data_750k = generate_friends_data(750000, users_data_750k)


# Generate data for 1,000,000 records

users_data_1m = generate_users_data(1000000)
posts_data_1m = generate_posts_data(1000000, users_data_1m)
messages_data_1m = generate_messages_data(1000000, users_data_1m)
friends_data_1m = generate_friends_data(1000000, users_data_1m)


def save_data_to_csv(data, file_name):
    with open(file_name, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerows(data)


# Save data for 250,000 records

save_data_to_csv(users_data_250k, 'users_250k.csv')
save_data_to_csv(posts_data_250k, 'posts_250k.csv')
save_data_to_csv(messages_data_250k, 'messages_250k.csv')
save_data_to_csv(friends_data_250k, 'friends_250k.csv')

# Save data for 500,000 records

save_data_to_csv(users_data_500k, 'users_500k.csv')
save_data_to_csv(posts_data_500k, 'posts_500k.csv')
save_data_to_csv(messages_data_500k, 'messages_500k.csv')
save_data_to_csv(friends_data_500k, 'friends_500k.csv')

# Save data for 750,000 records

save_data_to_csv(users_data_750k, 'users_750k.csv')
save_data_to_csv(posts_data_750k, 'posts_750k.csv')
save_data_to_csv(messages_data_750k, 'messages_750k.csv')
save_data_to_csv(friends_data_750k, 'friends_750k.csv')

# Save data for 1,000,000 records

save_data_to_csv(users_data_1m, 'users_1m.csv')
save_data_to_csv(posts_data_1m, 'posts_1m.csv')
save_data_to_csv(messages_data_1m, 'messages_1m.csv')
save_data_to_csv(friends_data_1m, 'friends_1m.csv')
