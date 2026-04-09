{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='status_change_event_key',
    on_schema_change='sync_all_columns',
    partition_by={
      "field": "event_datetime",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by = ["pk_user", "prev_level", "next_level"]
) }}

WITH dedup_status_change_events AS (
    SELECT
        status.*
    FROM {{ ref('stg_status_change_events') }} AS status
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            status.event_datetime,
            status.user_id,
            status.prev_level
        ORDER BY status.event_datetime
    ) = 1
),

fact_status_candidates AS (
    SELECT
        status.event_datetime,
        status.user_id,
        users.pk_user,
        status.prev_level,
        CASE
            WHEN status.prev_level = 'free' THEN 'paid'
            ELSE 'free'
        END AS next_level,
        ROW_NUMBER() OVER (
            PARTITION BY
                status.event_datetime,
                status.user_id,
                status.prev_level
            ORDER BY users.row_effective_datetime DESC
        ) AS rn
    FROM dedup_status_change_events AS status
    LEFT JOIN {{ ref('dim_users') }} AS users
        ON status.user_id = users.user_id
            AND status.event_datetime >= users.row_effective_datetime
            AND status.event_datetime < users.row_expiry_datetime
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'CAST(event_datetime AS STRING)',
        'CAST(user_id AS STRING)',
        'prev_level'
    ]) }} AS status_change_event_key,
    event_datetime,
    pk_user,
    prev_level,
    next_level
FROM fact_status_candidates
WHERE rn = 1
