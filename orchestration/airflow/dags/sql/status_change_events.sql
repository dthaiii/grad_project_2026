INSERT {{ BIGQUERY_DATASET }}.{{ STATUS_CHANGE_EVENTS_TABLE }}
SELECT
    ts,
    userId,
    level,
    city,
    zip,
    state,
    userAgent,
    lon,
    lat,
    lastName,
    firstName,
    gender,
    registration
FROM {{ BIGQUERY_DATASET }}.{{ STATUS_CHANGE_EVENTS_TABLE}}_{{ logical_date.strftime("%m%d%H") }}