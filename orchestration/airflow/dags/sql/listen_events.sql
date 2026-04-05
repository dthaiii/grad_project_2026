INSERT {{ BIGQUERY_DATASET }}.{{ LISTEN_EVENTS_TABLE }}
SELECT
    COALESCE(artist, 'NA') AS artist,
    COALESCE(song, 'NA') AS song,
    COALESCE(duration, -1.0) AS duration,
    ts,
    sessionId,
    COALESCE(auth, 'NA') AS auth,
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
    COALESCE(registration, 0) AS registration
FROM {{ BIGQUERY_DATASET }}.{{ LISTEN_EVENTS_TABLE}}_{{ logical_date.strftime("%m%d%H") }}