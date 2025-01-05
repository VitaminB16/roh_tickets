CREATE OR REPLACE EXTERNAL TABLE `clean.ext_roh_events`
(
  type STRING,
  productionId INT64,
  sourceType STRING,
  carouselDescription STRING,
  slug STRING,
  startTime STRING,
  endTime STRING,
  isHiddenFromTicketsAndEvents BOOL,
  locationId STRING,
  performanceType STRING,
  timestamp INT64,
  day STRING,
  url STRING,
  performanceId STRING
)
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
);
