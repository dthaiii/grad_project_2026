{{ config(
    materialized='incremental',
    unique_key = ['event_datetime', 'user_id']
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
    LOWER(level) AS prev_level
FROM
    {{ source(env_var('DBT_BIGQUERY_DATASET'), 'status_change_events_ext') }}
{% if is_incremental() %}

    WHERE date >= "{{ max_ingestion_datetime }}"
        AND {{ dbt_date.from_unixtimestamp("ts", format="milliseconds") }} > (
            SELECT COALESCE(MAX(this_table.event_datetime), "1900-01-01") - INTERVAL 10 MINUTE
            FROM {{ this }} AS this_table
        )

{% endif %}
