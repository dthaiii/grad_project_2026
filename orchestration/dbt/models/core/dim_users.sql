{{ config(materialized='table') }}

WITH final_change AS (
    SELECT
        user_id,
        MAX(event_datetime) AS max_event_datetime
    FROM
        {{ ref('stg_status_change_events') }}
    GROUP BY user_id
),

final_state AS (
    SELECT
        status.user_id,
        CASE
            WHEN status.prev_level = 'free' THEN 'paid'
            ELSE 'free'
        END AS level,
        status.event_datetime AS row_effective_datetime,
        CAST('9999-01-01' AS TIMESTAMP) AS row_expiry_datetime
    FROM
        final_change
    INNER JOIN {{ ref('stg_status_change_events') }} AS status
        ON status.event_datetime = final_change.max_event_datetime
            AND status.user_id = final_change.user_id
),

user_info AS (
    SELECT DISTINCT
        user_id,
        first_name,
        last_name,
        gender,
        registration_datetime
    FROM
        {{ ref('stg_users') }}
),

dim_user_no_pk AS (
    -- gets the final state for all users whose status has changed
    SELECT
        user_info.user_id,
        user_info.first_name,
        user_info.last_name,
        user_info.gender,
        user_info.registration_datetime,
        final_state.level,
        final_state.row_effective_datetime,
        final_state.row_expiry_datetime
    FROM
        final_state
    INNER JOIN user_info
        ON final_state.user_id = user_info.user_id

    UNION ALL

    -- gets all except the final state for all users whose status has changed
    -- and the only state for users whose status has never changed
    SELECT
        users.user_id,
        users.first_name,
        users.last_name,
        users.gender,
        users.registration_datetime,
        COALESCE(status.prev_level, users.level) AS level,
        COALESCE(
            LAG(status.event_datetime, 1) OVER (
                PARTITION BY users.user_id
                ORDER BY status.event_datetime ASC
            ), users.registration_datetime
        ) AS row_effective_datetime,
        COALESCE(status.event_datetime, '9999-01-01') AS row_expiry_datetime
    FROM
        {{ ref('stg_users') }} AS users
    LEFT JOIN {{ ref('stg_status_change_events') }} AS status
        ON users.user_id = status.user_id
    WHERE status.event_datetime IS NULL
        OR status.event_datetime >= users.event_datetime
)

SELECT
    {{ dbt_utils.generate_surrogate_key(["user_id", "row_effective_datetime"]) }} AS pk_user,
    *
FROM
    dim_user_no_pk
