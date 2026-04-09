{{ config(
    materialized='table',
    cluster_by = ["user_id", "session_start_datetime"]
) }}

WITH session_events AS (
    SELECT
        user_id,
        session_id,
        MIN(event_datetime) AS session_start_datetime,
        MAX(event_datetime) AS session_end_datetime,
        TIMESTAMP_DIFF(MAX(event_datetime), MIN(event_datetime), MINUTE) AS session_duration_minutes,
        COUNT(*) AS total_events_in_session
    FROM {{ ref('stg_page_view_events') }}
    GROUP BY 1, 2
)

SELECT
    *,
    CASE 
        WHEN session_duration_minutes >= 30 THEN 'High Quality'
        WHEN session_duration_minutes >= 10 THEN 'Medium Quality'
        ELSE 'Short Session'
    END AS session_quality_tier,
    CASE
        WHEN total_events_in_session > 50 THEN 'Highly Active'
        ELSE 'Passive'
    END AS user_engagement_level
FROM session_events