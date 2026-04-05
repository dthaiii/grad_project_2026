INSERT {{ BIGQUERY_DATASET }}.{{ AUTH_EVENTS_TABLE }}
SELECT
    ts,
    sessionId,
    COALESCE(level, 'NA') AS level,
    itemInSession,
    COALESCE(city, 'NA') AS city,
    COALESCE(zip, 0) AS zip,
    COALESCE(state, 'NA') AS state,
    COALESCE(userAgent, 'NA') AS userAgent,
    COALESCE(lon, 0.0) AS lon,
    COALESCE(lat, 0.0) AS lat,
    COALESCE(userId, 0) AS userId,
    COALESCE(lastName, 'NA') AS lastName,
    COALESCE(firstName, 'NA') AS firstName,
    COALESCE(gender, 'NA') AS gender,
    COALESCE(registration, 0) AS registration,
    COALESCE(success, FALSE) AS success
FROM {{ BIGQUERY_DATASET }}.{{ AUTH_EVENTS_TABLE}}_{{ logical_date.strftime("%m%d%H") }}