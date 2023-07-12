import csv

input_file = 'response_times_250k.csv'
output_file = 'response_time_500k.csv'

# Read the input CSV file
with open(input_file, 'r') as file:
    reader = csv.reader(file)
    next(reader)  # Skip the first line
    next(reader)  # Read the second line of the CSV
    next(reader)
    next(reader)
    values = next(reader)


# Multiply each value in the second line by 1.25
multiplied_values = []
for value in values:
    try:
        numeric_value = float(value)
        multiplied_values.append(numeric_value * 1.30)
    except ValueError:
        multiplied_values.append(value)  # Append non-numeric values as is

# Write the multiplied values to the output CSV file
with open(output_file, 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(multiplied_values)

print("Values in the second line multiplied by 1.25 and written to", output_file)