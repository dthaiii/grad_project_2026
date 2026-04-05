INSERT {{ BIGQUERY_DATASET }}.{{ SESSION_START_END_EVENTS_TABLE }}
SELECT
    ts,
    sessionId,
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
FROM {{ BIGQUERY_DATASET }}.{{ SESSION_START_END_EVENTS_TABLE}}_{{ logical_date.strftime("%m%d%H") }}