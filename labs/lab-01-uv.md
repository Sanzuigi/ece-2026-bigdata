# UV Lab: Python Project and Dataset Generation

Roy Homsi, Big Data Framework

[Lab subject](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/03.object-storage/lab-1-uv.md)

## Project setup

I created the project in `/home/onyxia/work/ece-2026-bigdata` in the Onyxia `vscode-pyspark` service. The aim was to generate two datasets, users and orders, with each order linked to a user through their UUID. These datasets became the input for the S3 lab.

I used Python 3.13 and uv, initialized Git on `main`, then created the package with `uv init --package` and added Faker. The dependencies are stored in `pyproject.toml` and their versions in `uv.lock`.

## Generators

The package is named `ece_2026_bigdata`:

- `serialize.py` prints CSV, a JSON array or JSON Lines. An empty output argument prints nothing.
- `dataset_users.py` generates 50 users by default, each with a UUID and a fictional Faker profile.
- `dataset_orders.py` generates between 0 and 100 orders per user, with a UUID, user UUID, date, quantity and product.

The timeline starts on January 1, 2020, in UTC. Each order is placed in the next hourly interval. Faker is seeded with 42, which gives the same identifiers and order values when the commands are repeated.

The two commands are declared in `pyproject.toml`:

```toml
[project.scripts]
dataset-users = "ece_2026_bigdata.dataset_users:main"
dataset-orders = "ece_2026_bigdata.dataset_orders:main"
```

## Execution

I ran the help commands, then generated a small dataset:

```bash
uv run dataset-users -h
uv run dataset-orders -h
uv run dataset-users -c 2 -o jsonline
uv run dataset-orders -u 2 -C 1 -c 2 -o json
```

The user command produced two records. The first user's UUID was `bdd640fb-0667-4ad1-9c80-317fa3b1799d`, and the second was `17be3111-1a2a-43ed-962b-0f79c37459ee`.

The order command produced three orders. Its first order referenced `bdd640fb-0667-4ad1-9c80-317fa3b1799d`, with `cookie` as the product and a quantity of 2. This is the link between the two datasets.

I also tried the three output formats and ran the commands twice to check they give the same output. With the default arguments, the generators produce 50 users and 2,829 orders.

## Git

I committed the Python package, project configuration, lock file and documentation to [ece-2026-bigdata](https://github.com/Sanzuigi/ece-2026-bigdata). Generated datasets and the virtual environment are excluded from Git.
