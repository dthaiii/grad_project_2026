{{ config(
    materialized='table',
    cluster_by = ["activity_date", "artist"]
) }}

WITH daily_listens AS (
    SELECT
        DATE(event_datetime) AS activity_date,
        song,
        artist,
        COUNT(*) AS play_count,
        COUNT(DISTINCT user_id) AS unique_listeners
    FROM {{ ref('stg_listen_events') }}
    GROUP BY 1, 2, 3
)

SELECT
    activity_date,
    song,
    artist,
    play_count,
    unique_listeners,
    DENSE_RANK() OVER (PARTITION BY activity_date ORDER BY play_count DESC) AS rank
FROM daily_listens
WHERE song != 'NA'