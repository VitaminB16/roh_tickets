CREATE OR REPLACE VIEW clean.v_roh_productions AS (
SELECT
  clean.URL_DECODE(location) AS location,
  date,
  SAFE.PARSE_TIME("%H:%M:%S.000000", clean.URL_DECODE(time)) AS time,
  clean.URL_DECODE(title) AS title,
  TIMESTAMP_SECONDS(CAST(CAST(timestamp AS INT64)/10e8 AS INT64)) AS timestamp,
  * EXCEPT(location, date, time, title, timestamp)
FROM
  clean.ext_roh_productions
)