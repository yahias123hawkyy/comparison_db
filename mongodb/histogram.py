import csv
import matplotlib.pyplot as plt
import pandas as pd


import openpyxl
from openpyxl.drawing.image import Image
import matplotlib.pyplot as plt
import numpy as np
from PIL import Image as PILImage

# # Load response times from files

files = [
    "response_times_250k.csv",
    "response_times_500k.csv",
    "response_times_750k.csv",
    "response_times_1m.csv"
]

allValues = []
for file in files:

    with open(file, 'r') as f:
        csv_reader = csv.reader(f)

        # Skip the first row
        next(csv_reader)

        # Read the second row
        second_row = next(csv_reader)

        # Ignore the first element (column) and process the remaining values
        values = second_row[1:]

        # Process the values as needed
        allValues.append(values)


def process_data(data):
    first_values = []  # without caching
    averages = []  # after caching

    for sublist in data:
        first_value = float(sublist[0])  # Convert the first value to a float
        first_values.append(first_value)

        # Convert remaining values to floats
        remaining_values = [float(value) for value in sublist[1:]]
        average = sum(remaining_values) / \
            len(remaining_values)  # Calculate the average
        averages.append(average)

    return first_values, averages


first_values, averages = process_data(allValues)


# Example data for the histogram

# Create a histogram using matplotlib
plt.hist(first_values, bins='auto')
plt.xlabel("Values")
plt.ylabel("Frequency")
plt.title("Histogram")

# Save the histogram as a PIL image
histogram_image_path = "histogram.png"
plt.savefig(histogram_image_path)
plt.close()

# Open the histogram image using PIL
pil_image = PILImage.open(histogram_image_path)

# Create a new workbook and select the active sheet
workbook = openpyxl.Workbook()
sheet = workbook.active

# Convert the PIL image to an openpyxl-compatible image
xl_image = Image(pil_image)

# Add the image to the worksheet
sheet.add_image(xl_image, "A1")

# Save the workbook
workbook.save("histogram.xlsx")
