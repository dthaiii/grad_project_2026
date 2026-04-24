{{ config(
    materialized='table',
    cluster_by = ["user_id", "activity_date"]
) }}

WITH page_counts AS (
    SELECT
        user_id,
        DATE(event_datetime) AS activity_date,
        COUNTIF(page = 'NextSong') AS next_song_clicks,
        COUNTIF(page = 'Settings') AS settings_clicks,
        COUNTIF(page = 'Help') AS help_clicks,
        COUNTIF(page = 'Error') AS error_count,
        COUNT(DISTINCT session_id) AS total_sessions
    FROM {{ ref('stg_page_view_events__fa') }}
    GROUP BY 1, 2
),

listen_stats AS (
    SELECT
        user_id,
        DATE(event_datetime) AS activity_date,
        SUM(duration) AS total_listening_time
    FROM {{ ref('stg_listen_events__fa') }}
    GROUP BY 1, 2
)

SELECT
    p.user_id,
    p.activity_date,
    p.next_song_clicks,
    p.settings_clicks,
    p.help_clicks,
    p.error_count,
    p.total_sessions,
    COALESCE(l.total_listening_time, 0) AS total_listening_time
FROM page_counts p
LEFT JOIN listen_stats l ON p.user_id = l.user_id AND p.activity_date = l.activity_date