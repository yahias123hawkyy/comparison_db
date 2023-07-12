import csv
import mysql.connector

# Establish MySQL connection
mysql_connection = mysql.connector.connect(
    host='localhost',
    user='root',
    password='12345678',
    database='social_media'
)

# Create MySQL cursor
cursor = mysql_connection.cursor()

# Set the SQL mode to allow the loading of a large number of rows
cursor.execute("SET GLOBAL local_infile = 1")

# Function to load data from CSV file using LOAD DATA INFILE
def load_data_from_csv(table_name, file_path):
    # Truncate the table (optional, if you want to start with an empty table)
    cursor.execute(f"TRUNCATE TABLE {table_name}")

    # Use LOAD DATA INFILE to perform bulk import
    load_query = f"LOAD DATA INFILE '{file_path}' INTO TABLE {table_name} \
                  FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n'"

    cursor.execute(load_query)

# CSV file paths
users_csv_file = '../datasets/k_500/users_500k.csv'
# posts_csv_file = '../datasets/k_500/posts_500k.csv'
# messages_csv_file = '../datasets/k_500/messages_500k.csv'
# friends_csv_file = '../datasets/k_500/friends_500k.csv'

# Load data from CSV files using bulk import
load_data_from_csv('users', users_csv_file)
# load_data_from_csv('posts', posts_csv_file)
# load_data_from_csv('messages', messages_csv_file)
# load_data_from_csv('connections', friends_csv_file)

# Commit the changes and close the MySQL connection
mysql_connection.commit()
cursor.close()
mysql_connection.close()
