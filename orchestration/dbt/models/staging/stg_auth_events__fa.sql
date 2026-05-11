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

WITH source_auth_events AS (
    SELECT
        {{ dbt_date.from_unixtimestamp("ts", format="milliseconds") }} AS event_datetime,
        TIMESTAMP_TRUNC({{ dbt_date.from_unixtimestamp("ts", format="milliseconds") }}, HOUR) AS bucket_ingestion_datetime,
        userid AS user_id,
        sessionid AS session_id,
        level,
        iteminsession AS item_in_session,
        NULLIF(NULLIF(CAST(zip AS STRING), '0'), '') AS postal_code,
        COALESCE(city, 'NO CITY') AS city,
        COALESCE(state, 'NO STATE') AS state,
        useragent AS user_agent,
        lon,
        lat,
        success,
        {{ dbt_date.from_unixtimestamp("ts", format="milliseconds") }} AS tf_sourcing_at,
        CURRENT_TIMESTAMP() AS tf_etl_at
    FROM {{ source('data_staging', 'auth_events') }}
    WHERE 1 = 1
    {% if is_incremental() %}
      AND {{ dbt_date.from_unixtimestamp("ts", format="milliseconds") }} > (
        SELECT COALESCE(MAX(event_datetime), TIMESTAMP('1900-01-01'))
        FROM {{ this }}
      )
    {% endif %}
)

SELECT * FROM source_auth_events
