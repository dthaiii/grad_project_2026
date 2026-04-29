{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    on_schema_change='append_new_columns',
    partition_by={
      "field": "event_datetime",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}

SELECT
    {{ dbt_date.from_unixtimestamp("ts", format="milliseconds") }} AS event_datetime,
    TIMESTAMP_TRUNC({{ dbt_date.from_unixtimestamp("ts", format="milliseconds") }}, HOUR) AS bucket_ingestion_datetime,
    userid AS user_id,
    LOWER(level) AS prev_level,
    {{ dbt_date.from_unixtimestamp("ts", format="milliseconds") }} AS tf_sourcing_at,
    CURRENT_TIMESTAMP() AS tf_etl_at
FROM {{ source('data_staging', 'status_change_events') }}
{% if is_incremental() %}
WHERE {{ dbt_date.from_unixtimestamp("ts", format="milliseconds") }} > (
    SELECT COALESCE(MAX(event_datetime), TIMESTAMP('1900-01-01'))
    FROM {{ this }}
)
{% endif %}
