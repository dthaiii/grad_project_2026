{{ config(
    materialized='incremental',
    unique_key = ['event_datetime', 'user_id', 'item_in_session'],
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
    sessionid AS session_id,
    iteminsession AS item_in_session,
    page,
    TRIM(useragent, '"') AS user_agent,
    ad_revenue
FROM
    {{ source(env_var('DBT_BIGQUERY_DATASET'), 'page_view_events_ext') }}
WHERE auth = "Logged In"
    {% if is_incremental() %}
    
        AND date >= "{{ max_ingestion_datetime }}"
        AND {{ dbt_date.from_unixtimestamp("ts", format="milliseconds") }} > (
            SELECT COALESCE(MAX(this_table.event_datetime), "1900-01-01") - INTERVAL 10 MINUTE
            FROM {{ this }} AS this_table
        )

    {% endif %}
