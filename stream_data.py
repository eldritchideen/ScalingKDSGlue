import boto3
import subprocess
import csv
import json
import time



kds_client = boto3.client("kinesis")
all_data = []

# Read in test data
with open("all_stocks_5yr.csv") as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        try:
            all_data.append(
                {
                    "date": row["date"],
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": int(row["volume"]),
                    "name": row["name"],
                }
            )
        except:
            continue


for data in all_data:
    resp = kds_client.put_record(StreamName="TestStream", Data=json.dumps(data), PartitionKey=data['name'])
    print(resp)
    time.sleep(0.1)