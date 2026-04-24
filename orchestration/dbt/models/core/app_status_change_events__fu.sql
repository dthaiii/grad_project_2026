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
    cluster_by=['pk_user', 'prev_level', 'next_level']
) }}

WITH dedup_status_change_events AS (
    SELECT
        status.*
    FROM {{ ref('stg_status_change_events__fa') }} AS status
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY status.event_datetime, status.user_id, status.prev_level
        ORDER BY status.event_datetime
    ) = 1
),

base_rows AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'CAST(status.event_datetime AS STRING)',
            'CAST(status.user_id AS STRING)',
            'status.prev_level'
        ]) }} AS status_change_event_key,
        status.event_datetime,
        users.pk_user,
        status.prev_level,
        CASE WHEN status.prev_level = 'free' THEN 'paid' ELSE 'free' END AS next_level,
        status.tf_sourcing_at
    FROM dedup_status_change_events AS status
    LEFT JOIN {{ ref('app_users__s2') }} AS users
      ON status.user_id = users.user_id
     AND status.event_datetime >= users.row_effective_datetime
     AND status.event_datetime < users.row_expiry_datetime
),

{% if is_incremental() %}
existing AS (
    SELECT status_change_event_key, tf_created_at
    FROM {{ this }}
)
{% endif %}

SELECT
    b.status_change_event_key,
    b.event_datetime,
    b.pk_user,
    b.prev_level,
    b.next_level,
    b.tf_sourcing_at,
    {% if is_incremental() %}
    COALESCE(e.tf_created_at, CURRENT_TIMESTAMP()) AS tf_created_at,
    {% else %}
    CURRENT_TIMESTAMP() AS tf_created_at,
    {% endif %}
    CURRENT_TIMESTAMP() AS tf_updated_at,
    CURRENT_TIMESTAMP() AS tf_etl_at,
    1 AS tf_record_status
FROM base_rows AS b
{% if is_incremental() %}
LEFT JOIN existing AS e USING (status_change_event_key)
{% endif %}
