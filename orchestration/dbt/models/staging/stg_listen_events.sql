{{ config(
    materialized='incremental',
    unique_key = ['event_datetime', 'user_id', 'song', 'artist'],
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

SELECT
    {{ dbt_date.from_unixtimestamp("ts", format="milliseconds") }} AS event_datetime,
    date AS bucket_ingestion_datetime,
    userid AS user_id,
    CAST(zip AS STRING) AS postal_code,
    COALESCE(city, "NO CITY") AS city,
    COALESCE(state, "NO STATE") AS state,
    level,
    {{ normalize_song_name('song') }} AS song,
    artist,
    ROUND(duration, 2) as duration
FROM
    {{ source(env_var('DBT_BIGQUERY_DATASET'), 'listen_events_ext') }}
{% if is_incremental() %}

    WHERE date >= "{{ max_ingestion_datetime }}"
        AND {{ dbt_date.from_unixtimestamp("ts", format="milliseconds") }} > (
            SELECT COALESCE(MAX(this_table.event_datetime), "1900-01-01") - INTERVAL 10 MINUTE
            FROM {{ this }} AS this_table
        )

{% endif %}
