CREATE OR REPLACE VIEW clean.v_roh_events AS (
SELECT
  clean.URL_DECODE(location) AS location,
  date,
  SAFE.PARSE_TIME("%H:%M:%S.000000", clean.URL_DECODE(time)) AS time,
  clean.URL_DECODE(title) AS title,
  * EXCEPT(location, date, time, title)
FROM
  clean.ext_roh_events
)