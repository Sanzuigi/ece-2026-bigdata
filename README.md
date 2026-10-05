# Big Data Framework Labs

Author: Roy Homsi

This repository contains my work for the ECE Big Data Framework course.
The Python project generates fictional users and orders for use in later data pipelines.

## Lab progress

- UV lab: implemented and verified. See [submission](labs/lab-01-uv.md).
- S3 lab: pending.

## Setup

The project uses Python 3.13 and uv. Install the locked dependencies:

```bash
uv sync --locked
```

## Usage

Display command help:

```bash
uv run dataset-users -h
uv run dataset-orders -h
```

Generate users and orders:

```bash
uv run dataset-users -c 50 -o json
uv run dataset-orders -u 50 -C 0 -c 100 -o jsonline
```

Both commands support `csv`, `json`, and `jsonline`.
Orders reference users through `user_uuid`.

Generate CSV datasets for the S3 lab:

```bash
uv run dataset-users -o csv > users.csv
uv run dataset-orders -o csv > orders.csv
```

Generated CSV files are excluded from Git.

## Verification

```bash
uv run python scripts/verify_uv.py
```

The script checks output formats, user/order links, order bounds, timestamps,
reproducibility across separate executions, defaults, and a custom start date.

## Repository structure

- `src/ece_2026_bigdata/`: generators and serialization.
- `scripts/verify_uv.py`: automated verification.
- `labs/`: individual lab write-ups.
- `pyproject.toml`: project metadata, dependencies, and commands.
- `uv.lock`: exact dependency versions.

## Saving and restoring work

Push commits to GitHub before terminating the Onyxia service.
To restore the project in a fresh service:

```bash
cd /home/onyxia/work
git clone https://github.com/Sanzuigi/ece-2026-bigdata.git
cd ece-2026-bigdata
uv sync --locked
```

Temporary platform credentials must be renewed separately.
