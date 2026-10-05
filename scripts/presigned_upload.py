import os
from pathlib import Path
from urllib.request import Request, urlopen

import boto3
from botocore.config import Config

session = boto3.Session(profile_name="default")
s3 = session.client(
    "s3",
    endpoint_url=os.environ["S3_ENDPOINT_URL"],
    config=Config(signature_version="s3v4"),
)
bucket = os.environ["LAB_BUCKET_NAME"]
key = "bronze/upload.csv"
data = Path("users.csv").read_bytes()

url = s3.generate_presigned_url(
    "put_object",
    Params={"Bucket": bucket, "Key": key},
    ExpiresIn=300,
)
request = Request(url, data=data, method="PUT")
with urlopen(request, timeout=60) as response:
    print(f"Presigned PUT HTTP status: {response.status}")

download = s3.get_object(Bucket=bucket, Key=key)
with download["Body"] as body:
    if body.read() != data:
        raise RuntimeError("Uploaded data differs from the local file.")
print("PASS: presigned PUT uploaded identical data")
