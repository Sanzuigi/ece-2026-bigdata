import csv
import io
import os
from pathlib import Path

import boto3

session = boto3.Session(profile_name="default")
s3 = session.client("s3", endpoint_url=os.environ["S3_ENDPOINT_URL"])
bucket = os.environ["LAB_BUCKET_NAME"]

datasets = {}
for name in ("users", "orders"):
    response = s3.get_object(Bucket=bucket, Key=f"bronze/{name}.csv")
    with response["Body"] as body:
        stored = body.read()
    if stored != Path(f"{name}.csv").read_bytes():
        raise RuntimeError(f"{name}.csv differs from the local dataset.")
    records = list(csv.DictReader(io.StringIO(stored.decode("utf-8"))))
    datasets[name] = records
    print(f"PASS: {name}.csv: {len(stored)} bytes, {len(records)} records, identical")

user_ids = {user["uuid"] for user in datasets["users"]}
if not all(order["user_uuid"] in user_ids for order in datasets["orders"]):
    raise RuntimeError("An order references a missing user.")
print("PASS: all stored orders reference stored users")
