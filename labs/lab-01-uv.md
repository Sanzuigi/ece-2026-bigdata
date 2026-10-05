# UV Lab: Python Project and Dataset Generation

Author: Roy Homsi  
Course: Big Data Framework  
Source: [lab-1-uv.md](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/03.object-storage/lab-1-uv.md)

## Objective

The aim of this lab was to create a packaged Python project with uv and use it to generate fictional users and orders. These two datasets are linked, as each order refers to a user, which provides the input needed for the S3 lab and the transformations in the following modules.

## Environment and project setup

I carried out the lab in the Onyxia `vscode-pyspark` service, with the project stored in its own directory at `/home/onyxia/work/ece-2026-bigdata`. Creating a separate directory allowed the Git repository to contain the project files without including the rest of the service's work directory.

The environment used Git 2.55.0, uv 0.12.10, Python 3.13.15, and Faker 40.40.0. I initialized Git on the `main` branch, created the project with `uv init --package`, and added Faker with `uv add faker`.

The project information and dependencies are stored in `pyproject.toml`, with the exact dependency versions recorded in `uv.lock`. This lock file is committed alongside the source code, since restoring the project should also restore the dependency versions used for the lab.

## Implementation

The package is named `ece_2026_bigdata` and contains three modules, each with a specific role in generating or printing the datasets.

Firstly, `serialize.py` handles the output, be it CSV, a JSON array, or JSON Lines. When the output argument is empty, the function prints nothing, which allows the order generator to call the user generator without also printing the user dataset.

`dataset_users.py` generates 50 users by default, with each user containing a UUID and a fictional profile provided by Faker. `dataset_orders.py` then generates between 0 and 100 orders per user by default, with each order containing its own UUID, the user's UUID, a date, a quantity, and a product.

The default timeline starts on January 1, 2020, in UTC. Each subsequent order is generated in the next hourly interval, which gives the orders a progression through time rather than placing them all within the same hour.

Faker is seeded with 42, so identical commands produce identical results across separate executions in the verified environment. The dependency versions are also locked, as using the same seed does not remove the need to control the environment in which the data is generated.

The command-line entry points are:

```toml
[project.scripts]
dataset-users = "ece_2026_bigdata.dataset_users:main"
dataset-orders = "ece_2026_bigdata.dataset_orders:main"
```

I also included checks rejecting negative user counts and invalid order bounds. Empty CSV datasets are handled without an indexing error, and timezone-aware start dates are converted to UTC, allowing the dates to follow the same timezone convention.

## Execution

I used the following commands to display the help information and generate small datasets:

```bash
uv run dataset-users -h
uv run dataset-orders -h
uv run dataset-users -c 2 -o jsonline
uv run dataset-orders -u 2 -C 1 -c 2 -o jsonline
```

This execution produced two users and three orders. The first user's UUID was `bdd640fb-0667-4ad1-9c80-317fa3b1799d`, and the first order referenced that same UUID, showing that the order was associated with the first generated user. Its product was `cookie`, with a quantity of 2.

## Verification results

To check the generators beyond this small example, I ran the verification script:

```bash
uv run python scripts/verify_uv.py
```

The execution returned:

```text
PASS: csv: formats, links, bounds, dates, reproducibility
PASS: json: formats, links, bounds, dates, reproducibility
PASS: jsonline: formats, links, bounds, dates, reproducibility
PASS: defaults: 50 users, 2829 linked orders
PASS: custom start date
All UV lab verification checks passed.
```

The script parses each output format, with CSV being read through a CSV reader since an address can contain line breaks within a single field. It checks that every order refers to a generated user, that quantities remain between 1 and 5, and that products belong to the predefined list.

It also checks that timestamps fall within the expected hourly intervals and that repeated commands give the same output. The default execution produced 50 users and 2,829 linked orders, with the custom start-date check also passing.

## Version control

The submission repository is [Sanzuigi/ece-2026-bigdata](https://github.com/Sanzuigi/ece-2026-bigdata).

I versioned the source code, documentation, project configuration, and lock file, as these are the files needed to restore and run the project. Virtual environments, generated datasets, and temporary credential files are excluded from Git, with the datasets being reproducible from the generators.

## Conclusion

The two generators and their command-line entry points were implemented and verified, with the checks confirming the output formats and the relationship between users and orders. The project can therefore provide the datasets needed for the S3 lab, while keeping the same environment through the committed dependency lock file.
