{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['user_id', 'tf_partition_date'],
    on_schema_change='sync_all_columns',
    partition_by={
      "field": "tf_partition_date",
      "data_type": "date",
      "granularity": "day"
    }
) }}

WITH all_status_events AS (
    SELECT
        user_id,
        DATE(event_datetime) AS tf_partition_date,
        prev_level,
        CASE
            WHEN prev_level = 'free' THEN 'paid'
            ELSE 'free'
        END AS curr_level,
        event_datetime AS latest_event_datetime
    FROM {{ ref('stg_status_change_events__fa') }}
),

status_by_day AS (
    SELECT
        user_id,
        tf_partition_date,
        prev_level,
        curr_level,
        latest_event_datetime
    FROM all_status_events
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY user_id, tf_partition_date
        ORDER BY latest_event_datetime DESC
    ) = 1
)

{% if is_incremental() %}
, existing AS (
    SELECT
        user_id,
        tf_partition_date,
        tf_created_at
    FROM {{ this }}
)
{% endif %}

SELECT
    src.user_id,
    src.tf_partition_date,
    src.prev_level,
    src.curr_level,
    src.latest_event_datetime,
    {% if is_incremental() %}
    COALESCE(ex.tf_created_at, CURRENT_TIMESTAMP()) AS tf_created_at,
    {% else %}
    CURRENT_TIMESTAMP() AS tf_created_at,
    {% endif %}
    CURRENT_TIMESTAMP() AS tf_updated_at,
    CURRENT_TIMESTAMP() AS tf_etl_at,
    1 AS tf_record_status
FROM status_by_day AS src
{% if is_incremental() %}
LEFT JOIN existing AS ex
    ON src.user_id = ex.user_id
   AND src.tf_partition_date = ex.tf_partition_date
{% endif %}
