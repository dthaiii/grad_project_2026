INSERT {{ BIGQUERY_DATASET }}.{{ USER_INFO_TABLE }}
SELECT
    COALESCE(SAFE_CAST(JSON_VALUE(TO_JSON_STRING(src), '$.userId') AS INT64), 0) AS userId,
    COALESCE(JSON_VALUE(TO_JSON_STRING(src), '$.lastName'), 'NA') AS lastName,
    COALESCE(JSON_VALUE(TO_JSON_STRING(src), '$.firstName'), 'NA') AS firstName,
    COALESCE(JSON_VALUE(TO_JSON_STRING(src), '$.gender'), 'NA') AS gender,
    COALESCE(SAFE_CAST(JSON_VALUE(TO_JSON_STRING(src), '$.registration') AS INT64), 0) AS registration,
    COALESCE(
        SAFE_CAST(JSON_VALUE(TO_JSON_STRING(src), '$.ts') AS INT64),
        UNIX_MILLIS(SAFE_CAST(JSON_VALUE(TO_JSON_STRING(src), '$.ts') AS TIMESTAMP)),
        0
    ) AS ts
FROM {{ BIGQUERY_DATASET }}.{{ USER_INFO_TABLE}}_{{ logical_date.strftime("%m%d%H") }} AS src