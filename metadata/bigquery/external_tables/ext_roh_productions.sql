CREATE OR REPLACE EXTERNAL TABLE clean.ext_roh_productions
WITH PARTITION COLUMNS(
    title STRING,
    productionId INT64,
    date DATE,
    time STRING,
    performanceId STRING
)
OPTIONS(
    format = 'PARQUET',
    uris = ['gs://vitaminb16-clean/output/roh_productions.parquet/*'],
    hive_partition_uri_prefix = 'gs://vitaminb16-clean/output/roh_productions.parquet/'
)
