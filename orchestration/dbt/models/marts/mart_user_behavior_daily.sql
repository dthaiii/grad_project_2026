{{ config(
    materialized='table',
    cluster_by = ["userId", "activity_date"]
) }}

WITH page_counts AS (
    SELECT
        userId,
        DATE(event_datetime) AS activity_date,
        COUNTIF(page = 'NextSong') AS next_song_clicks,
        COUNTIF(page = 'Settings') AS settings_clicks,
        COUNTIF(page = 'Help') AS help_clicks,
        COUNTIF(page = 'Error') AS error_count,
        COUNT(DISTINCT sessionId) AS total_sessions
    FROM {{ ref('stg_page_view_events') }}
    GROUP BY 1, 2
),

listen_stats AS (
    SELECT
        userId,
        DATE(event_datetime) AS activity_date,
        SUM(duration) AS total_listening_time
    FROM {{ ref('stg_listen_events') }}
    GROUP BY 1, 2
)

SELECT
    p.userId,
    p.activity_date,
    p.next_song_clicks,
    p.settings_clicks,
    p.help_clicks,
    p.error_count,
    p.total_sessions,
    COALESCE(l.total_listening_time, 0) AS total_listening_time
FROM page_counts p
LEFT JOIN listen_stats l ON p.userId = l.userId AND p.activity_date = l.activity_date
ORDER BY p.activity_date DESC, p.userId ASC