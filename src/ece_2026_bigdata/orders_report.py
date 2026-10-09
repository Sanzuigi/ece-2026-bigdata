import argparse
import os

import duckdb


def orders_report(product=None):
    bronze = f"s3://{os.environ['LAB_BUCKET_NAME']}/bronze"
    with duckdb.connect() as con:
        con.execute("SET TimeZone = 'UTC'")
        return con.execute(
            """
            SELECT strftime(date, '%Y-%m') AS month,
                   count(*) AS orders,
                   sum(quantity) AS quantity
            FROM read_csv($path, strict_mode = false)
            WHERE $product IS NULL OR product = $product
            GROUP BY month
            ORDER BY month
            """,
            {"path": f"{bronze}/orders.csv", "product": product},
        ).fetchall()


def main():
    parser = argparse.ArgumentParser(prog="orders-report", description="Monthly orders report")
    parser.add_argument("-p", "--product", help="Filter the orders on a product.")
    args = parser.parse_args()
    for month, orders, quantity in orders_report(args.product):
        print(f"{month}\t{orders}\t{quantity}")


if __name__ == "__main__":
    main()
