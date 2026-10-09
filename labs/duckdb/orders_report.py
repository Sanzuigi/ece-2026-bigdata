"""Monthly report; install as <package>.orders_report:main."""
import argparse
import os
import duckdb


def orders_report(product=None, source=None):
    bucket = os.environ.get('LAB_BUCKET_NAME') or os.environ.get('KUBERNETES_NAMESPACE')
    if not bucket and not source:
        raise RuntimeError('Set LAB_BUCKET_NAME to your S3 bucket name.')
    with duckdb.connect() as con:
        con.execute("SET TimeZone='UTC'")
        return con.execute(
            """SELECT strftime(date,'%Y-%m') AS month, count(*) AS orders,
                      sum(quantity) AS quantity
               FROM read_csv($path, strict_mode=false)
               WHERE $product IS NULL OR product=$product
               GROUP BY month ORDER BY month""",
            {'path': source or f's3://{bucket}/bronze/orders.csv', 'product': product},
        ).fetchall()


def main():
    parser = argparse.ArgumentParser(prog='orders-report', description='Monthly orders report')
    parser.add_argument('-p', '--product', help='Filter orders on a product.')
    parser.add_argument('--source', help='Optional CSV path for a local validation run.')
    args = parser.parse_args()
    for month, orders, quantity in orders_report(args.product, args.source):
        print(f'{month}\t{orders}\t{quantity}')


if __name__ == '__main__':
    main()
