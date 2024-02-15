CREATE OR REPLACE EXTERNAL TABLE clean.ext_roh_events
WITH PARTITION COLUMNS(
    location STRING,
    date DATE,
    time STRING,
    title STRING
)
OPTIONS(
    format = 'PARQUET',
    uris = ['gs://vitaminb16-clean/output/roh_events.parquet/*'],
    hive_partition_uri_prefix = 'gs://vitaminb16-clean/output/roh_events.parquet/'
)
