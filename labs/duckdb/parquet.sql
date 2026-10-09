-- Parquet export
COPY orders TO (getvariable('bucket') || '/analytics/orders.parquet') (FORMAT parquet);

-- Column metadata
SELECT
    path_in_schema,
    type,
    compression,
    total_compressed_size,
    total_uncompressed_size
FROM parquet_metadata(getvariable('bucket') || '/analytics/orders.parquet');

-- Product partitions
COPY orders TO (getvariable('bucket') || '/analytics/orders_by_product')
(FORMAT parquet, PARTITION_BY (product));

SELECT file
FROM glob(getvariable('bucket') || '/analytics/orders_by_product/**');

-- Partition pruning
EXPLAIN ANALYZE
SELECT count(*)
FROM read_parquet(getvariable('bucket') || '/analytics/orders_by_product/*/*.parquet')
WHERE product = 'cookie';
