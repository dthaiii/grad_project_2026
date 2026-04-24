{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    partition_by={
      "field": "event_datetime",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}

SELECT
    event_datetime,
    user_id,
    session_id,
    item_in_session,
    song,
    artist,
    duration,
    CURRENT_TIMESTAMP() AS tf_etl_at
FROM {{ ref('stg_page_view_events') }}
WHERE page = 'NextSong'
{% if is_incremental() %}
  AND event_datetime > (
      SELECT COALESCE(MAX(event_datetime), TIMESTAMP('1900-01-01'))
      FROM {{ this }}
  )
{% endif %}
