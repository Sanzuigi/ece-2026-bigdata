# Big Data Framework Labs

Author: Roy Homsi

This repository contains my work for the ECE Big Data Framework course, with the first two labs focusing on generating datasets in Python and storing them in S3. The users and orders are fictional, but their relationship is kept through a user UUID, which allows the datasets to be used together in the following modules.

## Lab progress

- UV lab: implemented and verified. See [submission](labs/lab-01-uv.md).
- S3 lab: executed and verified. See [submission](labs/lab-02-s3.md).

## Setup

The project uses Python 3.13 and uv, with the dependency versions recorded in `uv.lock`. Installing from this lock file allows the project to use the same versions as the ones used during the labs.

```bash
uv sync --locked
```

## Usage

The two commands have a help option showing their available arguments:

```bash
uv run dataset-users -h
uv run dataset-orders -h
```

The number of users, the number of orders per user, and the output format can be changed through these arguments. For example:

```bash
uv run dataset-users -c 50 -o json
uv run dataset-orders -u 50 -C 0 -c 100 -o jsonline
```

Both commands support `csv`, `json`, and `jsonline`, with each order containing a `user_uuid` referring to a generated user. This relationship is what allows an order to be associated with the person who placed it.

For the S3 lab, the datasets are generated as CSV files:

```bash
uv run dataset-users -o csv > users.csv
uv run dataset-orders -o csv > orders.csv
```

These CSV files are excluded from Git, as they can be generated again from the source code. The final copies are kept in S3 for the following labs.

## Verification

The verification script can be run with:

```bash
uv run python scripts/verify_uv.py
```

It checks the three output formats, the links between users and orders, the order bounds and timestamps, as well as the default arguments and a custom start date. It also compares separate executions of the same commands, which checks that the generated datasets remain reproducible in the tested environment.

## Repository structure

- `src/ece_2026_bigdata/`: the generators and serialization module.
- `scripts/verify_uv.py`: the automated checks for the UV lab.
- `labs/`: the individual lab write-ups.
- `pyproject.toml`: the project information, dependencies, and commands.
- `uv.lock`: the exact dependency versions.

## Saving and restoring work

The Onyxia work directory is temporary, so commits must be pushed to GitHub before terminating the service. A local commit alone would still be lost with that directory. In a fresh service, the project can be restored with:

```bash
cd /home/onyxia/work
git clone https://github.com/Sanzuigi/ece-2026-bigdata.git
cd ece-2026-bigdata
uv sync --locked
```

The platform credentials must be renewed separately, since they are temporary and are not stored in the repository.

## S3 lab scripts

The S3 scripts require the Onyxia default AWS profile and valid platform credentials. The endpoint and bucket must also be set in the terminal before running the SDK scripts:

```bash
export AWS_PAGER=""
export S3_ENDPOINT_URL="$(aws configure get endpoint_url --profile default)"
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
```

- Install s5cmd: `bash scripts/install_s5cmd.sh`.
- Test a presigned upload: `uv run --with boto3 python scripts/presigned_upload.py`.
- Run ingestion: `bash scripts/upload_bronze.sh`.
- Verify stored datasets: `uv run --with boto3 python scripts/verify_bronze.py`.

Before running ingestion again, the Job, ConfigMaps, and Secret from a previous run must have been removed, as the script creates those resources under the same names. Verification compares the stored datasets with the local CSV files.

The final users and orders datasets remain in the S3 bronze layer. The exercise cleanup script deletes selected keys, including `bronze/users.csv`, so it should only be used when deleting those objects is intended.
