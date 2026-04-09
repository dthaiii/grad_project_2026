{{ config(
    materialized='table',
    partition_by={
      "field": "activity_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=["user_id", "subscription_segment"]
) }}

WITH behavior AS (
    SELECT
        user_id,
        activity_date,
        next_song_clicks,
        settings_clicks,
        help_clicks,
        error_count,
        total_sessions,
        total_listening_time
    FROM {{ ref('mart_user_behavior_daily') }}
),

listening AS (
    SELECT
        user_id,
        activity_date,
        total_songs_listened,
        total_listening_minutes,
        unique_artists_listened
    FROM {{ ref('mart_user_listening_stats') }}
),

subscription AS (
    SELECT
        user_id,
        user_subscription_segment,
        days_to_upgrade
    FROM {{ ref('mart_user_subscription_journey') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'CAST(behavior.user_id AS STRING)',
        'CAST(behavior.activity_date AS STRING)'
    ]) }} AS user_activity_fact_key,
    behavior.user_id,
    behavior.activity_date,
    COALESCE(subscription.user_subscription_segment, 'Unknown') AS subscription_segment,
    subscription.days_to_upgrade,
    behavior.total_sessions,
    behavior.next_song_clicks,
    behavior.settings_clicks,
    behavior.help_clicks,
    behavior.error_count,
    COALESCE(listening.total_songs_listened, 0) AS total_songs_listened,
    COALESCE(listening.unique_artists_listened, 0) AS unique_artists_listened,
    COALESCE(listening.total_listening_minutes, 0.0) AS total_listening_minutes,
    COALESCE(behavior.total_listening_time, 0.0) AS total_listening_time_seconds,
    CASE
        WHEN behavior.total_sessions = 0 THEN 0.0
        ELSE ROUND(behavior.next_song_clicks / behavior.total_sessions, 2)
    END AS avg_next_song_clicks_per_session
FROM behavior
LEFT JOIN listening
    ON behavior.user_id = listening.user_id
    AND behavior.activity_date = listening.activity_date
LEFT JOIN subscription
    ON behavior.user_id = subscription.user_id
