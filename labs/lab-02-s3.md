# S3 Lab: Object Storage and Kubernetes Ingestion

Author: Roy Homsi  
Course: Big Data Framework  
Source: [lab-2-s3.md](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/03.object-storage/lab-2-s3.md)

## Objective

The aim of this lab was to explore how S3 stores and manages objects, then use a Kubernetes Job to upload the datasets from the UV lab into the bronze layer. The first part focused on individual storage operations, with the second bringing those operations into an ingestion task running inside the cluster.

## Environment

I carried out the lab in the Onyxia `vscode-pyspark` service, with `user-r-homsi-ece` being both my namespace and personal bucket name.

The tools used were AWS CLI 2.36.40, kubectl client 1.37.0, and s5cmd 2.3.0, alongside Python and uv from the UV lab. I installed s5cmd after checking the release archive's checksum, while boto3 was supplied temporarily through `uv run --with boto3` for the Python SDK operations.

Before creating the Kubernetes resources, I checked the permissions for Jobs, ConfigMaps, and Secrets. All three checks returned `yes`, which confirmed that the service could create the resources needed for the final exercise.

## S3 configuration

The service had supplied AWS credentials and the endpoint through environment variables, but the default profile files were missing. As a result, the first commands returned “The config profile (default) could not be found”, even though the credential variables were present.

I created a default profile under `~/.aws` using the supplied environment variables, without printing the credentials. The files were restricted to the owner with permissions `0600`, and the bucket listing then succeeded.

These credentials remain temporary, since copying them into a profile does not extend their validity. The credential files are outside the project repository and are not committed to Git.

## Object operations

The user generator produced a `users.csv` file containing 50 users and occupying 7,351 bytes. I uploaded this file to `bronze/users.csv`, then listed it with both AWS CLI and s5cmd, with both tools returning the same size.

I downloaded a copy and compared it with the local file using `diff`. The files were identical, which confirmed that the upload and download had preserved the dataset. I then moved the object to `bronze/users_renamed.csv`, listed and deleted it, before uploading it again under its original key.

S3 identifies objects through their keys, with the slash in `bronze/users.csv` providing a prefix convention rather than creating a filesystem directory. Moving an object therefore involves copying it and deleting the original, which means the rename is not atomic.

## Metadata

The uploaded object's ETag was:

`36aa2c0461b8c3498456bf92ad68bbb7`

This matched the local MD5 for the tested upload. Even though the two values matched here, an ETag should not be treated as a universal MD5 checksum, as multipart uploads and different encryption configurations can change how it is calculated.

I then uploaded the file with an explicit `text/csv` content type and descriptive metadata. The returned user metadata were:

```json
{
  "rows": "50",
  "source": "dataset_users.py",
  "version": "1"
}
```

These values describe the object and its source, but they do not provide a search index across the bucket. Updating the metadata also requires another upload or a copy with replacement metadata, as it cannot be edited independently in place.

## Presigned URLs

I generated a presigned GET URL with a 300-second lifetime and used it to download the user dataset without supplying credentials to the HTTP request. The downloaded file matched the local copy.

For the upload, `scripts/presigned_upload.py` generated a presigned PUT URL with boto3, uploaded the dataset, and checked its content afterwards. The request returned HTTP 200, with the stored data being identical to the local file.

The signed URLs were not printed or committed, since they temporarily authorize a specific operation for whoever holds the URL. Expiration behavior was not separately timed during this execution.

## Multipart upload

I uploaded a 200 MiB file to `large/dataset.bin`, with the recorded size being 209,715,200 bytes. Its ETag ended in `-25`, indicating that the upload used 25 parts.

The multipart listing contained no incomplete uploads. Splitting a large file into parts allows them to be transferred in parallel, with a failed part being retried separately rather than requiring the whole file to be uploaded again.

## Versioning

After enabling versioning, I uploaded the 50-user dataset and then replaced it with a 40-user version. Both versions appeared in the listing, with sizes of 7,351 and 5,855 bytes respectively.

I retrieved the original 50-user version through its version ID and compared it with the local file. They were identical, showing that replacing the current object had kept the previous data available.

Deleting the current object then created a delete marker while retaining the older versions. Another upload restored the current dataset. The listing also contained a `null` version, which came from the object stored before versioning was enabled.

## Lifecycle configuration

I applied the rules in `infrastructure/lifecycle.json` and retrieved the configuration to check that they had been recorded. The three rules were set to expire objects under `large/` after 30 days, delete non-current versions after 7 days, and abort incomplete multipart uploads after 7 days.

These rules allow the storage backend to manage retention and unfinished transfers automatically. Their configuration was verified immediately, but the scheduled actions were not observed over their multi-day retention periods.

## Access control

The bucket initially had no policy, and the object ACL showed an `admin` owner with `FULL_CONTROL`. I read the ACL without applying a public one.

I then applied a temporary policy denying `s3:DeleteObject` under `bronze/` and attempted to delete `bronze/protected.csv`. The request returned `AccessDenied`, which confirmed that the policy was enforced for this request. I removed the policy after the test, with the reusable version kept in `infrastructure/policy.template.json`.

## Immediate retrieval and exercise cleanup

An immediate download after an upload matched the local dataset. The earlier download to `/dev/null` had succeeded but returned a warning about updating its timestamp, so I repeated the verification with a regular file.

This confirmed that the tested object could be retrieved immediately after the upload. It does not establish every consistency guarantee of the backend, since the check only covers the operation carried out during this lab.

I then used the cleanup script to delete all versions and delete markers for the four exercise keys: `bronze/users.csv`, `bronze/upload.csv`, `bronze/protected.csv`, and `large/dataset.bin`. The script targeted these exact keys, leaving unrelated objects outside its scope.

Versioning was suspended and the lifecycle configuration was removed before moving on to the Kubernetes upload.

## Kubernetes ingestion

The final exercise used `scripts/upload_bronze.sh` to generate both datasets and create the resources needed for ingestion. The `datasets` ConfigMap mounted the CSV files under `/data`, while `s3-config` supplied the endpoint, region, and bucket name. The temporary credentials were stored in the `s3-credentials` Secret, with the `upload-bronze` Job running the AWS CLI container to carry out the upload.

The two datasets had a combined size of 338,919 bytes, which was below the 1 MiB ConfigMap limit and allowed them to be supplied through this mechanism for the lab.

The initial Job could not create a Pod because the platform quota required `limits.cpu`. I added a CPU limit of `200m` to the manifest and recreated the Job, which then completed successfully.

The resulting manifest requests `100m` of CPU and `128Mi` of memory, with limits of `200m` and `256Mi`. It uses the lab's `amazon/aws-cli:latest` image, although a production deployment should pin an approved version or digest to control which image is run.

After the Job uploaded both datasets, I ran `scripts/verify_bronze.py` and obtained:

```text
PASS: users.csv: 7351 bytes, 50 records, identical
PASS: orders.csv: 331568 bytes, 2829 records, identical
PASS: all stored orders reference stored users
```

Both stored files matched their local copies, and every stored order referred to one of the stored users. I then deleted the Job, both ConfigMaps, and the Secret, while retaining the two bronze datasets for the following modules.

## Questions

### Why use a Secret rather than a ConfigMap for credentials?

A ConfigMap is intended for non-sensitive configuration, such as the endpoint or bucket name, whereas credentials need to be handled as sensitive values. A Secret provides a dedicated mechanism for those values, which can be protected through access controls and encryption at rest.

However, base64 encoding alone is not encryption, so using a Secret does not remove the need to control access to it. The credential values must also stay out of Git, since versioning the configuration should not expose the credentials used to access the storage.

### What happens if the Job runs tomorrow, and how should production provide credentials?

The copied Onyxia credentials may have expired by tomorrow, which would cause the Job's authentication to fail even if its manifest had not changed. Running it again would therefore require a Secret populated with current credentials.

In production, workload identity or an equivalent mechanism should provide short-lived credentials automatically. This allows the Job to obtain the access it needs without embedding static keys in its manifest.

### How would this become daily ingestion?

The Job could be turned into a CronJob with a daily schedule, such as `0 2 * * *`, and an explicit timezone. Its Job template would then run the ingestion at the scheduled time.

Nevertheless, scheduling the existing ConfigMap alone would only upload the same snapshot again each day. The process would need fresh source data and current credentials for each run, as well as retries, control over overlapping runs, and a way to report failures.

## Submission and final state

The repository contains the source code, reusable scripts, infrastructure configuration, and this write-up. Credentials and generated CSV files are excluded from Git, while the bucket retains `bronze/users.csv` with 50 users and `bronze/orders.csv` with 2,829 orders.

The cleanup script must not be run again unless deleting its exercise keys is intended, since `bronze/users.csv` is now one of the retained inputs for future labs. The final datasets therefore remain available in S3, with the temporary Kubernetes resources having been removed after verification.
