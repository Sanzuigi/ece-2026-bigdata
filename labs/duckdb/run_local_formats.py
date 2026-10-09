"""Local format/API validation while S3 is unavailable. Never claims S3 measurements."""
import json
from pathlib import Path
import subprocess
import run_lab as lab


def main():
    lab.EVIDENCE.mkdir(parents=True, exist_ok=True)
    local_bucket = lab.LAB / 'local-bucket'
    (local_bucket / 'analytics').mkdir(parents=True, exist_ok=True)
    lab.sql((lab.LAB / 'parquet.sql').read_text(), 'local-parquet.txt', database='analytics-local.duckdb', init='init-local.sql')
    with open('orders_large.csv', 'w', encoding='utf-8', newline='') as out:
        result = subprocess.run(['uv', 'run', 'dataset-orders', '-u', '5000', '-o', 'csv'], stdout=out, stderr=subprocess.PIPE, text=True)
    (lab.EVIDENCE / 'local-generation.txt').write_text(result.stderr)
    if result.returncode:
        raise RuntimeError('Generator failed')
    lab.sql("COPY (FROM read_csv('orders_large.csv',strict_mode=false)) TO 'orders_large.parquet' (FORMAT parquet)", 'local-large-conversion.txt', init='init-local.sql')
    count = lab.sql("SELECT count(*) AS orders FROM read_parquet('orders_large.parquet')", 'local-large-count.json', json_output=True, init='init-local.sql')[0]['orders']
    queries = {
        'csv': "SELECT product,sum(quantity) FROM read_csv('orders_large.csv',strict_mode=false) GROUP BY product",
        'parquet': "SELECT product,sum(quantity) FROM read_parquet('orders_large.parquet') GROUP BY product",
        'filtered-parquet': "SELECT count(*) FROM read_parquet('orders_large.parquet') WHERE date >= '2100-01-01'",
    }
    for name, query in queries.items():
        lab.sql('SET enable_external_file_cache=false; EXPLAIN ANALYZE ' + query, 'local-benchmark-' + name + '.txt', init='init-local.sql')
    groups = lab.sql("SELECT count(*) AS total_row_groups,count(*) FILTER (WHERE try_cast(stats_max AS TIMESTAMPTZ) < TIMESTAMPTZ '2100-01-01 00:00:00+00') AS skipped_row_groups FROM parquet_metadata('orders_large.parquet') WHERE path_in_schema='date'", 'local-row-group-summary.json', json_output=True, init='init-local.sql')[0]
    csv = lab.sql(queries['csv'] + ' ORDER BY product', 'local-large-csv-totals.json', json_output=True, init='init-local.sql')
    parquet = lab.sql(queries['parquet'] + ' ORDER BY product', 'local-large-parquet-totals.json', json_output=True, init='init-local.sql')
    assert csv == parquet, 'CSV and Parquet totals differ'
    full = lab.run(['uv','run','orders-report','--source','orders.csv'], 'local-python-all.txt')
    lab.run(['uv','run','orders-report','--source','orders.csv','-p','cookie'], 'local-python-cookie.txt')
    unknown = lab.run(['uv','run','orders-report','--source','orders.csv','-p',"cookie' OR 1=1 --"], 'local-python-parameter-check.txt')
    assert not unknown.strip()
    reference = lab.sql("SELECT strftime(date,'%Y-%m') AS month,count(*) AS orders,sum(quantity) AS quantity FROM orders GROUP BY month ORDER BY month", 'local-monthly-reference.json', database='analytics-local.duckdb', json_output=True, init='init-local.sql')
    assert full == ''.join(f"{r['month']}\t{r['orders']}\t{r['quantity']}\n" for r in reference)
    sizes = {'csv_bytes':Path('orders_large.csv').stat().st_size,'parquet_bytes':Path('orders_large.parquet').stat().st_size}
    summary = {'mode':'LOCAL ONLY; S3 validation and HTTP transfer measurements pending','large_orders':count,**groups,**sizes,'csv_parquet_totals_match':True,'python_matches_sql':True}
    (lab.EVIDENCE / 'local-format-summary.json').write_text(json.dumps(summary,indent=2))
    print(json.dumps(summary,indent=2))


if __name__ == '__main__':
    main()
