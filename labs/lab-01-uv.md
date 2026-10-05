# UV Lab: Python Project and Dataset Generation

Author: Roy Homsi  
Course: Big Data Framework  
Source: [lab-1-uv.md](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/03.object-storage/lab-1-uv.md)

## Objective

Create a packaged Python project with uv and generate fictional users and orders.
These datasets provide the input for the S3 lab and subsequent transformations.

## Environment and project setup

The work was performed in the Onyxia `vscode-pyspark` service.
A dedicated repository directory was created at
`/home/onyxia/work/ece-2026-bigdata`.

The environment used Git 2.55.0, uv 0.12.10, Python 3.13.15,
and Faker 40.40.0.

Git was initialized on the `main` branch. The project was created using
`uv init --package`, and Faker was installed using `uv add faker`.
Project metadata and dependencies are stored in `pyproject.toml`.
The committed `uv.lock` records the exact dependency versions.

## Implementation

The Python package is named `ece_2026_bigdata`.

`serialize.py` prints records as CSV, a JSON array, or JSON Lines.
An empty output argument suppresses printing, allowing one generator to call another.

`dataset_users.py` generates 50 users by default. Each user contains a UUID
and a fictional profile supplied by Faker.

`dataset_orders.py` generates between 0 and 100 orders per user by default.
Each order contains a UUID, user UUID, date, quantity, and product.
The default timeline starts on January 1, 2020, in UTC. Each subsequent
order is generated in the next hourly interval.

Faker is seeded with 42. Identical commands produce identical results
across separate executions in the verified environment.
Dependency versions are locked to support reproducibility.

The command-line entry points are:

```toml
[project.scripts]
dataset-users = "ece_2026_bigdata.dataset_users:main"
dataset-orders = "ece_2026_bigdata.dataset_orders:main"
```

The implementation also rejects negative user counts and invalid order bounds,
handles empty CSV datasets, and converts timezone-aware start dates to UTC.

## Execution

```bash
uv run dataset-users -h
uv run dataset-orders -h
uv run dataset-users -c 2 -o jsonline
uv run dataset-orders -u 2 -C 1 -c 2 -o jsonline
```

The sample execution produced two users and three orders.
The first user UUID was `bdd640fb-0667-4ad1-9c80-317fa3b1799d`.
The first order referenced that same UUID, confirming the relationship.
Its product was `cookie` and its quantity was 2.

## Verification results

The following command was executed successfully:

```bash
uv run python scripts/verify_uv.py
```

Observed output:

```text
PASS: csv: formats, links, bounds, dates, reproducibility
PASS: json: formats, links, bounds, dates, reproducibility
PASS: jsonline: formats, links, bounds, dates, reproducibility
PASS: defaults: 50 users, 2829 linked orders
PASS: custom start date
All UV lab verification checks passed.
```

The checks parse CSV with a CSV reader, including addresses containing line breaks.
They verify that every order references a generated user, quantities stay between
1 and 5, products belong to the predefined list, and timestamps fall within
the expected hourly intervals.

## Version control

The submission repository is
[Sanzuigi/ece-2026-bigdata](https://github.com/Sanzuigi/ece-2026-bigdata).

Source code, documentation, project configuration, and the lock file are versioned.
Virtual environments, generated datasets, and temporary credential files are excluded.

## Conclusion

The generators and their command-line entry points are implemented and verified.
The project is ready to supply the user and order datasets required by the S3 lab.
