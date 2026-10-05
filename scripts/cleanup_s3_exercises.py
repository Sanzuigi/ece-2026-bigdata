import os

import boto3

bucket = os.environ["LAB_BUCKET_NAME"]
if bucket != os.environ["KUBERNETES_NAMESPACE"]:
    raise SystemExit("Bucket does not match your Onyxia namespace.")

session = boto3.Session(profile_name="default")
s3 = session.client("s3", endpoint_url=os.environ["S3_ENDPOINT_URL"])
keys = (
    "bronze/users.csv",
    "bronze/upload.csv",
    "bronze/protected.csv",
    "large/dataset.bin",
)
paginator = s3.get_paginator("list_object_versions")

for key in keys:
    objects = []
    for page in paginator.paginate(Bucket=bucket, Prefix=key):
        for category in ("Versions", "DeleteMarkers"):
            for item in page.get(category, []):
                if item["Key"] == key:
                    objects.append({
                        "Key": key,
                        "VersionId": item["VersionId"],
                    })
    for offset in range(0, len(objects), 1000):
        result = s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": objects[offset:offset + 1000], "Quiet": True},
        )
        if result.get("Errors"):
            raise RuntimeError(result["Errors"])
    print(f"Cleaned {key}: {len(objects)} versions/delete markers")

print("S3 exercise objects cleaned.")
