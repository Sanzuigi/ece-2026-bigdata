# S3 Lab: Object Storage and Kubernetes Ingestion

Author: Roy Homsi  
Course: Big Data Framework  
Source: [lab-2-s3.md](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/03.object-storage/lab-2-s3.md)

## Objective

Explore S3 object operations and storage management, then upload the datasets
from the UV lab through a Kubernetes Job into the bronze layer.

## Environment

The lab was executed in the Onyxia `vscode-pyspark` service.
The namespace and personal bucket were both `user-r-homsi-ece`.

Tools used:
- AWS CLI 2.36.40.
- kubectl client 1.37.0.
- s5cmd 2.3.0, installed after checking the release archive checksum.
- Python and uv from the UV lab.
- boto3, supplied temporarily through `uv run --with boto3`.

Kubernetes permission checks returned `yes` for creating Jobs, ConfigMaps,
and Secrets.

## S3 configuration

The service supplied AWS credentials and the endpoint through environment
variables, but the default profile files were missing. Initial commands
returned “The config profile (default) could not be found”.

A default profile was created under `~/.aws` from the supplied environment
variables, without printing credentials. The files were restricted to the
owner with permissions `0600`. Bucket listing then succeeded.

Credentials remain temporary. Creating a profile does not extend their validity.
Credential files are outside the project repository and are not committed.

## Object operations

The user generator produced `users.csv`, containing 50 users and occupying
7,351 bytes. The file was uploaded to `bronze/users.csv`.

Both AWS CLI and s5cmd listed the object. A downloaded copy was compared
with the local file using `diff`; the files were identical.

The object was moved to `bronze/users_renamed.csv`, listed, deleted,
and uploaded again under its original key.

S3 stores objects by key. A slash in a key provides a prefix convention,
rather than creating a filesystem directory. Moving an object performs
a copy followed by deletion, rather than an atomic rename.

## Metadata

The uploaded object's ETag was:

`36aa2c0461b8c3498456bf92ad68bbb7`

It matched the local MD5 for this upload. ETags should not be treated as
universal MD5 checksums, particularly for multipart uploads or differing
encryption configurations.

An upload with explicit `text/csv` content type returned these user metadata:

```json
{
  "rows": "50",
  "source": "dataset_users.py",
  "version": "1"
}
```

Metadata describes an object but does not provide a bucket-wide search index.
Updating metadata requires an upload or copy with replacement metadata.

## Presigned URLs

A presigned GET URL with a 300-second lifetime downloaded an identical copy
of the user dataset.

`scripts/presigned_upload.py` generated a presigned PUT URL with boto3,
uploaded the dataset, and verified its content. The HTTP response was 200.

The signed URLs were not printed or committed. These URLs authorize a
specific operation temporarily. Expiration behavior was not separately timed
in this execution.

## Multipart upload

A 200 MiB file was uploaded to `large/dataset.bin`. Its recorded size was
209,715,200 bytes. Its ETag ended with `-25`, indicating 25 parts.

The multipart listing contained no incomplete uploads. Multipart uploads
allow parallel transfer and retries of individual parts.

## Versioning

Versioning was enabled. The 50-user dataset was uploaded, followed by a
40-user replacement. Their sizes were 7,351 and 5,855 bytes respectively.

The original 50-user version was retrieved by its version ID and matched
the local file. Deleting the current object created a delete marker while
retaining the older versions. Another upload restored the current dataset.

The listing also contained a `null` version from before versioning was enabled.

## Lifecycle configuration

The rules in `infrastructure/lifecycle.json` were applied and retrieved:

- Expire objects under `large/` after 30 days.
- Delete non-current versions after 7 days.
- Abort incomplete multipart uploads after 7 days.

The configuration was verified immediately. The scheduled actions were not
observed over their multi-day retention periods.

## Access control

The bucket initially had no policy. The object ACL showed an `admin` owner
with `FULL_CONTROL`. No public ACL was applied.

A temporary policy denied `s3:DeleteObject` under `bronze/`.
Deleting `bronze/protected.csv` returned `AccessDenied`, confirming that
the policy was enforced for the tested request. The policy was then removed.

The reusable policy template is `infrastructure/policy.template.json`.

## Immediate retrieval and exercise cleanup

An immediate download after upload matched the local dataset. An earlier
download to `/dev/null` succeeded but reported a timestamp-update warning;
verification was repeated using a regular file.

This confirms the observed immediate retrieval behavior for the tested object,
rather than establishing every consistency guarantee of the backend.

The cleanup script deleted all versions and delete markers for the four
exercise keys: `bronze/users.csv`, `bronze/upload.csv`,
`bronze/protected.csv`, and `large/dataset.bin`.

Versioning was suspended and the lifecycle configuration was removed.
Unrelated object keys were not targeted.

## Kubernetes ingestion

`scripts/upload_bronze.sh` generated the datasets and created:

- `datasets`: a ConfigMap mounting the CSV files under `/data`.
- `s3-config`: a ConfigMap containing the endpoint, region, and bucket name.
- `s3-credentials`: a Secret containing temporary AWS credentials.
- `upload-bronze`: a Job running the AWS CLI container.

The combined dataset size was 338,919 bytes, below the 1 MiB ConfigMap limit.

The initial Job could not create a Pod because the platform quota required
`limits.cpu`. A CPU limit of `200m` was added to the manifest, and the Job
was recreated successfully.

The resulting manifest uses CPU and memory requests of `100m` and `128Mi`,
with limits of `200m` and `256Mi`. It uses the lab's `amazon/aws-cli:latest`
image; a production deployment should pin an approved image version or digest.

The Job completed and uploaded both datasets. Verification with
`scripts/verify_bronze.py` returned:

```text
PASS: users.csv: 7351 bytes, 50 records, identical
PASS: orders.csv: 331568 bytes, 2829 records, identical
PASS: all stored orders reference stored users
```

The Job, both ConfigMaps, and the Secret were deleted after verification.
The two bronze datasets were retained for future modules.

## Questions

### Why use a Secret rather than a ConfigMap for credentials?

ConfigMaps are intended for non-sensitive configuration. Secrets provide
a dedicated mechanism for sensitive values and can be protected through
access controls and encryption at rest. Base64 encoding alone is not
encryption. Credential values must not be committed to Git.

### What happens if the Job runs tomorrow, and how should production provide credentials?

The copied Onyxia credentials may have expired, causing authentication failure.
A new Job would require a Secret populated with current credentials.

A production platform should use workload identity or an equivalent mechanism
to provide short-lived credentials automatically, without embedding static
keys in manifests.

### How would this become daily ingestion?

Use a CronJob with a daily schedule, such as `0 2 * * *`, and an explicit
timezone. Its Job template would perform the ingestion.

The process must obtain current credentials and fresh source data for every
run. Scheduling this fixed ConfigMap alone would repeatedly upload the same
snapshot. A production process should also handle retries, avoid unwanted
overlapping runs, and report failures.

## Submission and final state

The repository contains source code, reusable scripts, infrastructure
configuration, and this write-up. Credentials and generated CSV files
are excluded from Git.

The bucket retains:
- `bronze/users.csv`: 50 users.
- `bronze/orders.csv`: 2,829 orders.

The cleanup script must not be rerun unless deletion of its exercise keys
is intended, because `bronze/users.csv` is now a retained input for future labs.
