{{ config(
    materialized='table',
    cluster_by = ["userId", "registration_datetime"]
) }}

WITH user_registration AS (
    SELECT
        user_id AS userId,
        MIN(registration_datetime) AS registration_datetime,
        MIN(first_name) AS first_name,
        MIN(last_name) AS last_name
    FROM {{ ref('stg_users') }}
    GROUP BY 1
),

upgrade_events AS (
    SELECT
        userId,
        MIN(event_datetime) AS subscription_upgrade_datetime
    FROM {{ ref('stg_status_change_events') }}
    WHERE LOWER(level) = 'paid'
    GROUP BY 1
)

SELECT
    reg.userId,
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
LEFT JOIN upgrade_events upg ON reg.userId = upg.userId
ORDER BY days_to_upgrade ASC NULLS LAST