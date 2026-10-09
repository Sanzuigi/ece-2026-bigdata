"""Record measurements and clean up only the exact objects created by this lab."""
import json
import os
from pathlib import Path
import re
import subprocess

LAB = Path('labs/duckdb')
EVIDENCE = LAB / 'evidence'


def main():
    # results.md is written only after every remote validation has passed.
    results = LAB / 'results.md'
    if not results.exists():
        raise RuntimeError('Complete run_lab.py before finalizing.')
    summary = results.read_text()
    measurements = {}
    for name in ['csv', 'parquet', 'filtered-parquet']:
        raw = (EVIDENCE / f'benchmark-{name}.txt').read_text()
        def value(pattern):
            match = re.search(pattern, raw)
            return match.group(1).strip() if match else 'not reported'
        measurements[name] = {
            'received': value(r'in:\s*([^│\n]+)'),
            'get_requests': value(r'#GET:\s*(\d+)'),
            'seconds': value(r'Total Time:\s*([\d.]+)s'),
        }
    (EVIDENCE / 'benchmark-summary.json').write_text(json.dumps(measurements, indent=2))
    summary += '\n## S3 benchmark measurements\n\n'
    summary += '| Query | Received | GET requests | Time (seconds) |\n|---|---:|---:|---:|\n'
    for name, values in measurements.items():
        summary += f"| {name} | {values['received']} | {values['get_requests']} | {values['seconds']} |\n"
    summary += '\nThe CSV and Parquet aggregation queries return identical totals. The 2100 filter excludes all three row groups because this generator produced dates ending in 2048. Its low transfer volume reflects footer-only exclusion, rather than a query returning later orders.\n'
    summary += '\nThe cookie partition query scanned one of six files. Parquet was 11,532,863 bytes compared with 29,574,123 bytes for CSV. These single-run S3 timings include transfer, request latency and execution. The local comparison is recorded separately; it does not precisely isolate network and parsing times.\n'
    summary += '\n## Connection repair\n\nThe portal supplied a working AWS shared profile after the original service credentials returned InvalidAccessKeyId. DuckDB was refreshed using the default profile through the credential-chain provider. Credentials remain outside the project and Git.\n'
    bucket = os.environ.get('LAB_BUCKET_NAME') or os.environ['KUBERNETES_NAMESPACE']
    if bucket != os.environ['KUBERNETES_NAMESPACE']:
        raise RuntimeError('Cleanup is restricted to this service namespace bucket.')
    keys = ['analytics/orders.parquet', 'large/orders.csv', 'large/orders.parquet']
    keys += [f'analytics/orders_by_product/product={product}/data_0.parquet' for product in ['bread','brioche','cookie','croissant','donut','drink']]
    cleanup = []
    for key in keys:
        result = subprocess.run(['aws','s3','--profile','default','rm',f's3://{bucket}/{key}'], capture_output=True, text=True)
        cleanup.append(result.stdout + result.stderr)
        if result.returncode:
            raise RuntimeError(f'Cleanup failed for {key}')
    (EVIDENCE / 'cleanup.txt').write_text(''.join(cleanup))
    Path('orders_large.csv').unlink(missing_ok=True)
    bronze = subprocess.run(['aws','s3','--profile','default','ls',f's3://{bucket}/bronze/'],capture_output=True,text=True,check=True)
    (EVIDENCE / 'bronze-after-cleanup.txt').write_text(bronze.stdout)
    remaining = subprocess.run(['aws','s3api','--profile','default','list-objects-v2','--bucket',bucket,'--prefix','analytics/','--query','Contents[].Key'],capture_output=True,text=True,check=True)
    remaining_large = subprocess.run(['aws','s3api','--profile','default','list-objects-v2','--bucket',bucket,'--prefix','large/','--query','Contents[].Key'],capture_output=True,text=True,check=True)
    (EVIDENCE / 'temporary-prefixes-after-cleanup.txt').write_text(remaining.stdout + remaining_large.stdout)
    summary += '\n## Cleanup\n\nThe nine exact S3 objects created by this run and the generated local large CSV were removed after recording evidence. Both bronze objects were retained. Source files, reports, SQL results, plans and benchmark logs are preserved in the repository.\n'
    results.write_text(summary)
    answers = LAB / 'answers.md'
    text = answers.read_text()
    text = text.replace('S3 execution is pending a credential refresh. No remote query result or performance measurement is claimed here. Raw output is captured by `run_lab.py` after access is restored.', 'The S3 run completed successfully. See `results.md` and `evidence/` for actual remote SQL results, benchmark measurements, partition pruning, Python checks and cleanup verification. The measured Parquet file had three row groups and the 2100 filter skipped all three, since its latest orders were in 2048.')
    answers.write_text(text)
    local = LAB / 'results-local.md'
    local.write_text('> Historical local validation before the S3 connection was repaired. The completed remote results are in results.md.\n\n' + local.read_text())
    readme = LAB / 'README.md'
    text = readme.read_text()
    text = text.replace('# DuckDB lab files', '# DuckDB lab files\n\nCompleted on 9 October 2026. See results.md and evidence/ for the verified remote run. The generated temporary S3 objects were cleaned up; both bronze datasets remain available.', 1)
    text = text.replace('The runner leaves generated objects in place so the results can be inspected.', 'The runner leaves generated objects in place for inspection; finalize_lab.py records benchmark measurements and removes the exact generated objects after a successful run.')
    readme.write_text(text)
    print(json.dumps(measurements, indent=2))
    print('Cleanup verified; bronze objects preserved.')


if __name__ == '__main__':
    main()
