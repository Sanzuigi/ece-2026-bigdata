COPY orders TO (getvariable('bucket') || '/analytics/orders.parquet') (FORMAT parquet);
SELECT path_in_schema,type,compression,total_compressed_size,total_uncompressed_size FROM parquet_metadata(getvariable('bucket') || '/analytics/orders.parquet');
-- OVERWRITE_OR_IGNORE supports reruns of this lab's own partition export.
COPY orders TO (getvariable('bucket') || '/analytics/orders_by_product') (FORMAT parquet,PARTITION_BY(product),OVERWRITE_OR_IGNORE);
SELECT file FROM glob(getvariable('bucket') || '/analytics/orders_by_product/**');
EXPLAIN ANALYZE SELECT count(*) FROM read_parquet(getvariable('bucket') || '/analytics/orders_by_product/*/*.parquet') WHERE product='cookie';
