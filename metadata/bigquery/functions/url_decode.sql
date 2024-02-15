CREATE OR REPLACE FUNCTION clean.URL_DECODE(encoded STRING)
RETURNS STRING
LANGUAGE js AS """
  try {
    return decodeURIComponent(encoded.replace(/\\+/g, ' '));
  } catch(e) {
    return encoded;
  }
""";
