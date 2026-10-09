# S3 Lab: Object Storage and Kubernetes Ingestion

[Lab subject](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/03.object-storage/lab-2-s3.md)

## Environment

I used the same Onyxia project as in the uv lab, with `user-r-homsi-ece` as my namespace and bucket. I worked with AWS CLI, s5cmd and kubectl, and used boto3 through `uv run --with boto3` for the Python operations.

The default AWS profile was initially missing. I configured it from the credentials supplied by Onyxia, then the bucket listing worked. Credentials are kept outside Git.

## Object operations

I generated `users.csv`, containing 50 users and occupying 7,351 bytes, and uploaded it to `bronze/users.csv`. AWS CLI and s5cmd both listed the same object size.

I downloaded the object and compared it with the local file using `diff`: the contents were identical. I then moved it to `bronze/users_renamed.csv`, listed and deleted it, and uploaded it again under the original key.

The slash in an S3 key defines a prefix, not a filesystem directory. A rename copies the object to another key and deletes the original.

## Metadata

The ETag of the tested upload was `36aa2c0461b8c3498456bf92ad68bbb7`, matching the local MD5. Multipart uploads use a different ETag format, as shown later in the lab.

I uploaded the file with the `text/csv` content type and the following metadata:

```json
{
  "rows": "50",
  "source": "dataset_users.py",
  "version": "1"
}
```

The metadata describe the object, but do not create a search index across the bucket. Updating them requires another upload or a copy with replacement metadata.

## Presigned URLs

I generated a GET URL with a 300-second lifetime and downloaded the dataset through HTTP. It matched the local file.

For PUT, I used `scripts/presigned_upload.py`. The upload returned HTTP 200, and the stored contents matched `users.csv`.

## Multipart upload

I uploaded a 200 MiB file to `large/dataset.bin`. Its size was 209,715,200 bytes, with an ETag ending in `-25`, indicating 25 parts. There were no incomplete uploads in the multipart listing.

This splits the transfer into smaller parts, so a failed part can be retried without uploading the whole file again.

## Versioning and lifecycle

After enabling versioning, I uploaded the 50-user dataset, then replaced it with a 40-user version. The listing contained both versions, with sizes of 7,351 and 5,855 bytes. I retrieved the original through its version ID and compared it with the local file: they were identical.

Deleting the current object created a delete marker while keeping the older versions. Another upload restored the current dataset. The listing also contained the `null` version from before versioning was enabled.

I applied and retrieved the rules in `infrastructure/lifecycle.json`:

- Expire objects under `large/` after 30 days.
- Delete non-current versions after 7 days.
- Abort incomplete multipart uploads after 7 days.

## Access control and cleanup

The bucket initially had no policy, and the object ACL gave the `admin` owner `FULL_CONTROL`. I applied a temporary policy denying `s3:DeleteObject` under `bronze/`, then attempted to delete `bronze/protected.csv`. The request returned `AccessDenied`. I removed the policy afterwards; its template is in `infrastructure/policy.template.json`.

An immediate download after an upload matched the local dataset.

I deleted the versions and delete markers for the exercise keys `bronze/users.csv`, `bronze/upload.csv`, `bronze/protected.csv` and `large/dataset.bin`. I suspended versioning and removed the lifecycle configuration before the Kubernetes ingestion.

## Kubernetes ingestion

I checked that my service could create Jobs, ConfigMaps and Secrets. The three permission checks returned `yes`.

In `scripts/upload_bronze.sh`, I generated both CSV files and created the Kubernetes resources. The `datasets` ConfigMap mounted the files under `/data`, `s3-config` supplied the endpoint, region and bucket, and `s3-credentials` stored the temporary credentials. The `upload-bronze` Job used the AWS CLI container to upload the files.

The files totalled 338,919 bytes, below the 1 MiB ConfigMap limit.

The first Job could not create a Pod because the quota required `limits.cpu`. I added a `200m` CPU limit and recreated it. The Job then completed. The manifest requests `100m` CPU and `128Mi` memory, with limits of `200m` and `256Mi`, and uses `amazon/aws-cli:latest`.

I downloaded and compared the uploaded datasets:

| Object | Size | Records |
|---|---:|---:|
| `bronze/users.csv` | 7,351 bytes | 50 |
| `bronze/orders.csv` | 331,568 bytes | 2,829 |

Both files matched the local copies, and every order referenced a stored user. I removed the Job, ConfigMaps and Secret, keeping the bronze objects for the following labs.

## Questions

### Why use a Secret for credentials?

A ConfigMap stores ordinary configuration. A Secret is intended for sensitive values and can be protected through RBAC and encryption at rest. Base64 encoding by itself is not encryption.

### What happens if the Job runs tomorrow?

The temporary Onyxia credentials may have expired, so the Job would need current credentials. In production, workload identity can provide short-lived credentials automatically instead of embedding static keys in the manifest.

### How would this become daily ingestion?

I would use a CronJob with a daily schedule such as `0 2 * * *` and an explicit timezone. The ingestion would also need fresh source data and credentials for each run; scheduling the same ConfigMap would only upload the same snapshot again.
