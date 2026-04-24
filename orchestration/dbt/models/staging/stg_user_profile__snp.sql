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
        tf_sourcing_at,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY tf_sourcing_at DESC) AS rn
    FROM {{ ref('stg_users__fu') }}
),

latest_user_zip AS (
    SELECT
        user_id,
        postal_code AS zip,
        event_datetime,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_datetime DESC) AS rn
    FROM {{ ref('stg_page_view_events__fa') }}
    WHERE postal_code IS NOT NULL
)

SELECT
    p.user_id,
    p.first_name,
    p.last_name,
    p.gender,
    z.zip,
    p.level,
    p.tf_sourcing_at,
    CURRENT_TIMESTAMP() AS tf_etl_at
FROM latest_user_profile AS p
LEFT JOIN latest_user_zip AS z
    ON p.user_id = z.user_id AND z.rn = 1
WHERE p.rn = 1
