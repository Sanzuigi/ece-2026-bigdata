"""Remove the nine temporary objects created by the DuckDB lab."""
import os
from pathlib import Path
import subprocess


def main():
    bucket = os.environ.get('LAB_BUCKET_NAME') or os.environ.get('KUBERNETES_NAMESPACE')
    if not bucket or bucket != os.environ.get('KUBERNETES_NAMESPACE'):
        raise RuntimeError('Cleanup requires the bucket of this Onyxia namespace.')
    keys = ['analytics/orders.parquet', 'large/orders.csv', 'large/orders.parquet']
    keys += [f'analytics/orders_by_product/product={product}/data_0.parquet'
             for product in ['bread', 'brioche', 'cookie', 'croissant', 'donut', 'drink']]
    for key in keys:
        subprocess.run(['aws', 's3', '--profile', 'default', 'rm', f's3://{bucket}/{key}'], check=True)
    Path('orders_large.csv').unlink(missing_ok=True)
    evidence = Path('labs/duckdb/evidence')
    evidence.mkdir(parents=True, exist_ok=True)
    bronze = subprocess.run(['aws', 's3', '--profile', 'default', 'ls', f's3://{bucket}/bronze/'],
                            capture_output=True, text=True, check=True)
    (evidence / 'bronze-after-cleanup.txt').write_text(bronze.stdout, encoding='utf-8')
    remaining = []
    for prefix in ['analytics/', 'large/']:
        result = subprocess.run(['aws', 's3api', '--profile', 'default', 'list-objects-v2',
                                 '--bucket', bucket, '--prefix', prefix, '--query', 'Contents[].Key'],
                                capture_output=True, text=True, check=True)
        remaining.append(result.stdout)
    (evidence / 'temporary-prefixes-after-cleanup.txt').write_text(''.join(remaining), encoding='utf-8')


if __name__ == '__main__':
    main()
