{{ config(
    materialized='table',
    cluster_by = ["user_id", "activity_date"]
) }}

WITH user_activity AS (
    SELECT
        user_id,
        DATE(event_datetime) AS activity_date,
        COUNT(song) AS total_songs_listened,
        SUM(duration) AS total_listening_time_seconds,
        COUNT(DISTINCT artist) AS unique_artists_listened
    FROM {{ ref('stg_listen_events__fa') }}
    GROUP BY 1, 2
),

user_demographics AS (
    SELECT
        user_id,
        first_name,
        last_name,
        gender,
        level AS user_tier
    FROM {{ ref('app_users__s2') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY row_effective_datetime DESC) = 1
)

SELECT
    u.user_id,
    u.first_name,
    u.last_name,
    u.gender,
    u.user_tier,
    a.activity_date,
    a.total_songs_listened,
    ROUND(a.total_listening_time_seconds / 60, 2) AS total_listening_minutes,
    a.unique_artists_listened
FROM user_demographics u
JOIN user_activity a ON u.user_id = a.user_id