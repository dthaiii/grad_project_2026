{{ config(
    materialized='table',
    cluster_by = ["user_id", "registration_datetime"]
) }}

WITH user_registration AS (
    SELECT
        user_id,
        MIN(registration_datetime) AS registration_datetime,
        MIN(first_name) AS first_name,
        MIN(last_name) AS last_name
    FROM {{ ref('stg_users') }}
    GROUP BY 1
),

upgrade_events AS (
    SELECT
        user_id,
        MIN(event_datetime) AS subscription_upgrade_datetime
    FROM {{ ref('stg_status_change_events') }}
    WHERE LOWER(prev_level) = 'free'
    GROUP BY 1
)

SELECT
    reg.user_id,
    reg.first_name,
    reg.last_name,
    reg.registration_datetime,
    upg.subscription_upgrade_datetime,
    TIMESTAMP_DIFF(upg.subscription_upgrade_datetime, reg.registration_datetime, DAY) AS days_to_upgrade,
    CASE 
        WHEN upg.subscription_upgrade_datetime IS NULL THEN 'Free Tier (Never Upgraded)'
        WHEN TIMESTAMP_DIFF(upg.subscription_upgrade_datetime, reg.registration_datetime, DAY) <= 7 THEN 'Fast Conversion (< 1 Week)'
        WHEN TIMESTAMP_DIFF(upg.subscription_upgrade_datetime, reg.registration_datetime, DAY) <= 30 THEN 'Standard Conversion (< 1 Month)'
        ELSE 'Long-term Conversion (> 1 Month)'
    END AS user_subscription_segment
FROM user_registration reg
LEFT JOIN upgrade_events upg ON reg.user_id = upg.user_id