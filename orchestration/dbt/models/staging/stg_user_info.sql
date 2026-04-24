{{ config(
    materialized='table',
    tags=['staging']
) }}

WITH latest_user_profile AS (
    SELECT
        user_id,
        first_name,
        last_name,
        gender,
        level,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY event_datetime DESC
        ) AS rn
    FROM {{ ref('stg_users') }}
),

latest_user_zip AS (
    SELECT
        user_id,
        postal_code AS zip,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY event_datetime DESC
        ) AS rn
    FROM {{ ref('stg_page_view_events') }}
    WHERE postal_code IS NOT NULL
        AND CAST(postal_code AS STRING) != '0'
)

SELECT
    p.user_id,
    p.first_name,
    p.last_name,
    p.gender,
    z.zip,
    p.level
FROM latest_user_profile AS p
LEFT JOIN latest_user_zip AS z
    ON p.user_id = z.user_id
    AND z.rn = 1
WHERE p.rn = 1