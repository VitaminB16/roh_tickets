CREATE OR REPLACE VIEW clean.v_roh_events AS (
SELECT
  clean.URL_DECODE(location) AS location,
  date,
  SAFE.PARSE_TIME("%H:%M:%S.000000", clean.URL_DECODE(time)) AS time,
  TIMESTAMP_MICROS(CAST(timestamp / 1000 AS INT64)) AS timestamp,
  clean.URL_DECODE(title) AS title,
  * EXCEPT(location, date, time, title, timestamp)
FROM
  clean.ext_roh_events
)