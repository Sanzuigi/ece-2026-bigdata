"""Execute the lab and retain real outputs; run from the uv project root."""
import json
import os
from pathlib import Path
import subprocess

ROOT = Path.cwd()
LAB = ROOT / 'labs' / 'duckdb'
EVIDENCE = LAB / 'evidence'


def run(args, name, *, stdin=None):
    result = subprocess.run(args, input=stdin, text=True, capture_output=True)
    (EVIDENCE / name).write_text(result.stdout + result.stderr, encoding='utf-8')
    if result.returncode:
        print(result.stdout + result.stderr)
        raise RuntimeError(f'{name} failed; see evidence log')
    print(f'OK {name}', flush=True)
    return result.stdout


def sql(query, name, *, database=None, json_output=False, init='init.sql'):
    args = ['duckdb']
    if database:
        args.append(database)
    args += ['-init', init, '-bail']
    if json_output:
        args.append('-json')
    args += ['-c', query]
    output = run(args, name)
    return json.loads(output) if json_output else output


def main():
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    bucket = os.environ.get('LAB_BUCKET_NAME') or os.environ.get('KUBERNETES_NAMESPACE')
    if not bucket:
        raise RuntimeError('Set LAB_BUCKET_NAME before starting.')
    os.environ['LAB_BUCKET_NAME'] = bucket
    bucket_uri = f's3://{bucket}'
    init = 'init.sql'
    database = 'analytics.duckdb'
    Path(init).write_text("SET VARIABLE bucket='" + bucket_uri.replace("'", "''") + "';\nSET TimeZone='UTC';\n", encoding='utf-8')
    sql((LAB / 'analytics.sql').read_text(), 'analytics.txt', database=database, init=init)
    checks = sql("""SELECT (SELECT count(*) FROM users) AS users,
        (SELECT count(*) FROM orders) AS orders,
        (SELECT count(*) FROM orders o ANTI JOIN users u ON o.user_uuid=u.uuid) AS orphan_orders,
        (SELECT count(*) FROM users u ANTI JOIN orders o ON o.user_uuid=u.uuid) AS inactive_users,
        (SELECT count(*) FROM (SELECT uuid FROM orders GROUP BY uuid HAVING count(*)>1)) AS duplicate_order_ids,
        (SELECT count(*) FROM orders)::DOUBLE / nullif((SELECT count(*) FROM users),0) AS avg_orders_per_user,
        (SELECT avg(quantity) FROM orders) AS avg_quantity_per_order""",
        'checks.json', database=database, json_output=True, init=init)[0]
    assert checks['orphan_orders'] == 0, checks
    assert checks['duplicate_order_ids'] == 0, checks
    print(json.dumps(checks), flush=True)
    sql((LAB / 'parquet.sql').read_text(), 'parquet.txt', database=database)
    run(['aws', 's3', '--profile', 'default', 'ls', f's3://{bucket}/analytics/', '--recursive'], 'analytics-sizes.txt')
    # Keep generator stdout separate from diagnostic stderr.
    with open('orders_large.csv', 'w', encoding='utf-8', newline='') as out:
        generated = subprocess.run(['uv', 'run', 'dataset-orders', '-u', '5000', '-o', 'csv'], stdout=out, stderr=subprocess.PIPE, text=True)
    (EVIDENCE / 'generation.txt').write_text(generated.stderr, encoding='utf-8')
    if generated.returncode:
        raise RuntimeError('Large dataset generation failed')
    run(['aws', 's3', '--profile', 'default', 'cp', 'orders_large.csv', f's3://{bucket}/large/orders.csv'], 'large-upload.txt')
    sql("COPY (FROM read_csv(getvariable('bucket') || '/large/orders.csv',strict_mode=false)) TO (getvariable('bucket') || '/large/orders.parquet') (FORMAT parquet);", 'large-conversion.txt')
    large_count = sql("SELECT count(*) AS orders FROM read_parquet(getvariable('bucket') || '/large/orders.parquet')", 'large-count.json', json_output=True)[0]['orders']
    run(['aws', 's3', '--profile', 'default', 'ls', f's3://{bucket}/large/'], 'large-sizes.txt')
    queries = {
        'csv': "SELECT product,sum(quantity) FROM read_csv(getvariable('bucket') || '/large/orders.csv',strict_mode=false) GROUP BY product",
        'parquet': "SELECT product,sum(quantity) FROM read_parquet(getvariable('bucket') || '/large/orders.parquet') GROUP BY product",
        'filtered-parquet': "SELECT count(*) FROM read_parquet(getvariable('bucket') || '/large/orders.parquet') WHERE date >= '2100-01-01'",
    }
    for name, query in queries.items():
        sql('SET enable_external_file_cache=false; EXPLAIN ANALYZE ' + query, 'benchmark-' + name + '.txt')
    sql("SELECT row_group_id,row_group_num_rows,stats_min,stats_max FROM parquet_metadata(getvariable('bucket') || '/large/orders.parquet') WHERE path_in_schema='date'", 'row-groups.json', json_output=True)
    groups = sql("""SELECT count(*) AS total_row_groups,
        count(*) FILTER (WHERE try_cast(stats_max AS TIMESTAMPTZ) < TIMESTAMPTZ '2100-01-01 00:00:00+00') AS skipped_row_groups
        FROM parquet_metadata(getvariable('bucket') || '/large/orders.parquet') WHERE path_in_schema='date'""", 'row-group-summary.json', json_output=True)[0]
    csv_totals = sql(queries['csv'] + ' ORDER BY product', 'large-csv-totals.json', json_output=True)
    parquet_totals = sql(queries['parquet'] + ' ORDER BY product', 'large-parquet-totals.json', json_output=True)
    assert csv_totals == parquet_totals, 'CSV/Parquet aggregates differ'
    run(['uv', 'run', 'orders-report'], 'python-all.txt')
    run(['uv', 'run', 'orders-report', '-p', 'cookie'], 'python-cookie.txt')
    unknown = run(['uv', 'run', 'orders-report', '-p', "cookie' OR 1=1 --"], 'python-parameter-check.txt')
    assert not unknown.strip(), 'Unexpected result for an unknown parameter value'
    expected = sql("SELECT strftime(date,'%Y-%m') AS month,count(*) AS orders,sum(quantity) AS quantity FROM orders GROUP BY month ORDER BY month", 'monthly-reference.json', database=database, json_output=True)
    expected_text = ''.join(f"{row['month']}\t{row['orders']}\t{row['quantity']}\n" for row in expected)
    assert (EVIDENCE / 'python-all.txt').read_text() == expected_text, 'Python report differs from SQL reference'
    print(f'Completed: {large_count} large orders; {groups}')



if __name__ == '__main__':
    main()
