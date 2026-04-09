{{ config(
    materialized='table',
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
    TIMESTAMP_TRUNC({{ dbt_date.from_unixtimestamp("ts", format="milliseconds") }}, HOUR) AS bucket_ingestion_datetime,
    userid AS user_id,
    CAST(zip AS STRING) AS postal_code,
    COALESCE(city, "NO CITY") AS city,
    COALESCE(state, "NO STATE") AS state,
    level,
    sessionid AS session_id,
    iteminsession AS item_in_session,
    page,
    TRIM(useragent, '"') AS user_agent,
    COALESCE(SAFE_CAST(JSON_VALUE(TO_JSON_STRING(t), '$.ad_revenue') AS FLOAT64), 0.0) AS ad_revenue
FROM
    {{ source(env_var('DBT_SOURCE_DATASET'), 'page_view_events') }} AS t
WHERE auth = "Logged In"
    {% if is_incremental() %}
    
        AND {{ dbt_date.from_unixtimestamp("ts", format="milliseconds") }} >= TIMESTAMP_SUB(TIMESTAMP("{{ max_ingestion_datetime }}"), INTERVAL 10 MINUTE)
        AND {{ dbt_date.from_unixtimestamp("ts", format="milliseconds") }} > (
            SELECT COALESCE(MAX(this_table.event_datetime), "1900-01-01") - INTERVAL 10 MINUTE
            FROM {{ this }} AS this_table
        )

    {% endif %}
