import pandas as pd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# InfluxDB configuration
INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = "sQY1wYw3yDcRg35YExT3GD9PCn_EPZOBW5hlNIdq5vVbK4VG4mGdv4sEqU6PtPfiQwBa2AIt6cin0VlrX4jNxQ=="
INFLUXDB_ORG = "51210a7db2211551"
INFLUXDB_BUCKET = "sample"

# CSV file path
# Replace with your CSV file path


def csv_to_influxdb(csv_data, influxdb_client):
    # Read CSV data into a pandas DataFrame
    data = pd.read_csv(csv_data)

    # Convert DataFrame to InfluxDB-compatible Point objects
    write_api = influxdb_client.write_api(write_options=SYNCHRONOUS)
    points = []
    for _, row in data.iterrows():
        point = Point(row['measurement']).time(
            row['timestamp'], WritePrecision.NS)
        for tag in row['tag_key=tag_value'].split(','):
            key, value = tag.split('=')
            point = point.tag(key, value)
        for field in row['field_key=field_value'].split(','):
            key, value = field.split('=')
            point = point.field(key, float(value))
        points.append(point)

    # Write the data to InfluxDB
    write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=points)
