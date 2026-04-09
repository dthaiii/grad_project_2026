INSERT {{ BIGQUERY_DATASET }}.{{ STATUS_CHANGE_EVENTS_TABLE }}
SELECT
    COALESCE(
        SAFE_CAST(JSON_VALUE(TO_JSON_STRING(src), '$.ts') AS INT64),
        UNIX_MILLIS(SAFE_CAST(JSON_VALUE(TO_JSON_STRING(src), '$.ts') AS TIMESTAMP)),
        0
    ) AS ts,
    COALESCE(SAFE_CAST(JSON_VALUE(TO_JSON_STRING(src), '$.sessionId') AS INT64), 0) AS sessionId,
    COALESCE(JSON_VALUE(TO_JSON_STRING(src), '$.auth'), 'NA') AS auth,
    COALESCE(JSON_VALUE(TO_JSON_STRING(src), '$.level'), 'NA') AS level,
    COALESCE(SAFE_CAST(JSON_VALUE(TO_JSON_STRING(src), '$.itemInSession') AS INT64), 0) AS itemInSession,
    COALESCE(JSON_VALUE(TO_JSON_STRING(src), '$.city'), 'NA') AS city,
    COALESCE(SAFE_CAST(JSON_VALUE(TO_JSON_STRING(src), '$.zip') AS INT64), 0) AS zip,
    COALESCE(JSON_VALUE(TO_JSON_STRING(src), '$.state'), 'NA') AS state,
    COALESCE(JSON_VALUE(TO_JSON_STRING(src), '$.userAgent'), 'NA') AS userAgent,
    COALESCE(SAFE_CAST(JSON_VALUE(TO_JSON_STRING(src), '$.lon') AS FLOAT64), 0.0) AS lon,
    COALESCE(SAFE_CAST(JSON_VALUE(TO_JSON_STRING(src), '$.lat') AS FLOAT64), 0.0) AS lat,
    COALESCE(SAFE_CAST(JSON_VALUE(TO_JSON_STRING(src), '$.userId') AS INT64), 0) AS userId,
    COALESCE(JSON_VALUE(TO_JSON_STRING(src), '$.lastName'), 'NA') AS lastName,
    COALESCE(JSON_VALUE(TO_JSON_STRING(src), '$.firstName'), 'NA') AS firstName,
    COALESCE(JSON_VALUE(TO_JSON_STRING(src), '$.gender'), 'NA') AS gender,
    COALESCE(SAFE_CAST(JSON_VALUE(TO_JSON_STRING(src), '$.registration') AS INT64), 0) AS registration
FROM {{ BIGQUERY_DATASET }}.{{ STATUS_CHANGE_EVENTS_TABLE}}_{{ logical_date.strftime("%m%d%H") }} AS src