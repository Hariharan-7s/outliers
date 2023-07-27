import csv
import pandas as pd
from pandas_insert import csv_to_influxdb


def points_to_csv(outliers, time_values, measurement, time_interval, infulxdb_client, output_file):
    # Create an empty string to store the CSV data
    csv_data = ""
    interval = get_time_interval_name(time_interval)
    # Write the header row
    csv_data += f"timestamp,measurement,tag_key=tag_value,field_key=field_value\n"

    # Iterate through each data point and timestamp and append them to the CSV data string
    for timestamp, value in zip(time_values, outliers):
        # Convert timestamp to string with desired format 'dd-mm-yyyy HH:MM'
        formatted_timestamp = timestamp.strftime('%Y-%m-%d %H:%M')

        # Appending the data point as a row to the CSV data string
        csv_data += f"{formatted_timestamp},{measurement}_outlier,interval={interval},value={value}\n"

    with open(output_file, mode='w', newline='') as csv_file:
        csv_file.write(csv_data)

    # Return the filename or file object

    csv_to_influxdb(output_file, infulxdb_client)


"""


def points_to_csv(outliers, time_values, measurement, time_interval, infulxdb_client):
    interval = get_time_interval_name(time_interval)
    data = []

    # Iterate through each data point and timestamp and append them to the data list
    for timestamp, value in zip(time_values, outliers):
        # Convert timestamp to string with desired format 'dd-mm-yyyy HH:MM'
        formatted_timestamp = timestamp.strftime('%Y-%m-%d %H:%M')

        # Append the data point as a row to the data list
        data.append({
            'timestamp': formatted_timestamp,
            'measurement': f"{measurement}_outlier",
            'interval': interval,
            'value': value
        })

    # Create a DataFrame from the data list
    df = pd.DataFrame(data)

    print(df)
"""


def get_time_interval_name(time_interval):
    # Convert the time interval to lowercase to handle variations in input
    time_interval = time_interval.lower()

    if time_interval == "1w":
        return "Week"
    elif time_interval == "1mo":
        return "Month"
    elif time_interval == "1d":
        return "Day"
    elif time_interval == "1h":
        return "Hour"
    else:
        # If the time interval is not recognized, return it as is
        return time_interval

# Example usage:


# Print the CSV data


# To write the CSV data to a file, you can use the following code:
# with open('output.csv', mode='w', newline='') as csv_file:
#     csv_file.write(csv_data)
