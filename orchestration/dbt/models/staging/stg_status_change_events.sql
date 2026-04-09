{{ config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['event_datetime', 'user_id', 'prev_level'],
        on_schema_change='sync_all_columns',
        partition_by={
            "field": "event_datetime",
            "data_type": "timestamp",
            "granularity": "day"
        }
) }}
 
{% if is_incremental() %}
    {%- set max_ingestion_datetime = get_value_from_query(
        "COALESCE(MAX(bucket_ingestion_datetime),'1900-01-01 00:00:00+00')",
        this) -%}
{% endif %}

WITH source_status_change_events AS (
    SELECT
        {{ dbt_date.from_unixtimestamp("ts", format="milliseconds") }} AS event_datetime,
        TIMESTAMP_TRUNC({{ dbt_date.from_unixtimestamp("ts", format="milliseconds") }}, HOUR) AS bucket_ingestion_datetime,
        userid AS user_id,
        LOWER(level) AS prev_level
    FROM
        {{ source(env_var('DBT_SOURCE_DATASET'), 'status_change_events') }}
    {% if is_incremental() %}

        WHERE {{ dbt_date.from_unixtimestamp("ts", format="milliseconds") }} >= TIMESTAMP_SUB(TIMESTAMP("{{ max_ingestion_datetime }}"), INTERVAL 10 MINUTE)
            AND {{ dbt_date.from_unixtimestamp("ts", format="milliseconds") }} > (
                SELECT COALESCE(MAX(this_table.event_datetime), "1900-01-01") - INTERVAL 10 MINUTE
                FROM {{ this }} AS this_table
            )

    {% endif %}
),

dedup_status_change_events AS (
    SELECT
        source.*
    FROM source_status_change_events AS source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            source.event_datetime,
            source.user_id,
            source.prev_level
        ORDER BY source.bucket_ingestion_datetime DESC, source.event_datetime DESC
    ) = 1
)

SELECT
    event_datetime,
    bucket_ingestion_datetime,
    user_id,
    prev_level
FROM dedup_status_change_events
