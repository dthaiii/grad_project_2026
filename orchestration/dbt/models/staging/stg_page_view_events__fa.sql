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
    {{ dbt_date.from_unixtimestamp("ts", format="milliseconds") }} AS event_datetime,
    TIMESTAMP_TRUNC({{ dbt_date.from_unixtimestamp("ts", format="milliseconds") }}, HOUR) AS bucket_ingestion_datetime,
    userid AS user_id,
    NULLIF(NULLIF(CAST(zip AS STRING), '0'), '') AS postal_code,
    COALESCE(city, 'NO CITY') AS city,
    COALESCE(state, 'NO STATE') AS state,
    level,
    sessionid AS session_id,
    iteminsession AS item_in_session,
    page,
    TRIM(useragent, '"') AS user_agent,
    CASE WHEN page = 'NextSong' AND level = 'free' THEN 0.005 ELSE 0.0 END AS ad_revenue,
    {{ dbt_date.from_unixtimestamp("ts", format="milliseconds") }} AS tf_sourcing_at,
    CURRENT_TIMESTAMP() AS tf_etl_at
FROM {{ source('data_staging', 'page_view_events') }}
WHERE auth = 'Logged In'
{% if is_incremental() %}
  AND {{ dbt_date.from_unixtimestamp("ts", format="milliseconds") }} > (
    SELECT COALESCE(MAX(event_datetime), TIMESTAMP('1900-01-01'))
    FROM {{ this }}
  )
{% endif %}
