(
     SELECT
   ARRAY_AGG(STRUCT(
     REGEXP_EXTRACT(y, '[^"]*') AS key
   , REGEXP_EXTRACT(y, r':([^"]*)\"?[,}\]]') AS value
   ))
  FROM UNNEST((
    SELECT REGEXP_EXTRACT_ALL(json,col_regex||r'[^:]+:\"?[^"]+\"?') arr
    FROM (SELECT TO_JSON_STRING(x) json))) y
)