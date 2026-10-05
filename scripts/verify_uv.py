import csv
import datetime
import io
import json
import subprocess
from collections import Counter


def run(command, *args):
    return subprocess.check_output(
        ["uv", "run", command, *args], text=True
    )


def parse(text, output):
    if output == "json":
        return json.loads(text)
    if output == "jsonline":
        return [json.loads(line) for line in text.splitlines()]
    return list(csv.DictReader(io.StringIO(text)))


for output in ("csv", "json", "jsonline"):
    user_args = ("-c", "2", "-o", output)
    order_args = ("-u", "2", "-C", "1", "-c", "2", "-o", output)

    user_text = run("dataset-users", *user_args)
    order_text = run("dataset-orders", *order_args)
    users = parse(user_text, output)
    orders = parse(order_text, output)

    assert len(users) == 2
    user_ids = {user["uuid"] for user in users}
    assert len(user_ids) == 2
    assert all(order["user_uuid"] in user_ids for order in orders)

    counts = Counter(order["user_uuid"] for order in orders)
    assert all(1 <= counts[user_id] <= 2 for user_id in user_ids)
    assert len({order["uuid"] for order in orders}) == len(orders)
    assert all(1 <= int(order["quantity"]) <= 5 for order in orders)
    products = {"bread", "brioche", "cookie", "croissant", "donut", "drink"}
    assert all(order["product"] in products for order in orders)

    start = datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC)
    for index, order in enumerate(orders):
        date = datetime.datetime.fromisoformat(order["date"])
        lower = start + datetime.timedelta(hours=index)
        upper = lower + datetime.timedelta(hours=1)
        assert lower <= date <= upper

    assert user_text == run("dataset-users", *user_args)
    assert order_text == run("dataset-orders", *order_args)
    print(f"PASS: {output}: formats, links, bounds, dates, reproducibility")

users = json.loads(run("dataset-users"))
orders = json.loads(run("dataset-orders"))
assert len(users) == 50
user_ids = {user["uuid"] for user in users}
assert all(order["user_uuid"] in user_ids for order in orders)
counts = Counter(order["user_uuid"] for order in orders)
assert all(0 <= counts[user_id] <= 100 for user_id in user_ids)
print(f"PASS: defaults: {len(users)} users, {len(orders)} linked orders")

custom = json.loads(run(
    "dataset-orders", "-u", "1", "-C", "1", "-c", "1",
    "-d", "2024-01-01"
))
date = datetime.datetime.fromisoformat(custom[0]["date"])
assert datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC) <= date
assert date <= datetime.datetime(2024, 1, 1, 1, tzinfo=datetime.UTC)
print("PASS: custom start date")
print("All UV lab verification checks passed.")
