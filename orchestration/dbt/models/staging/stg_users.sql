{{ config(
    materialized='incremental',
    unique_key = ['user_id']
) }}

{% if is_incremental() %}
    {%- set max_ingestion_datetime = get_value_from_query(
        "COALESCE(MAX(bucket_ingestion_datetime),'1900-01-01 00:00:00+00')",
        this) -%}
{% endif %}

SELECT
    {{ dbt_date.from_unixtimestamp("min(ts)", format="milliseconds") }} AS event_datetime,
    TIMESTAMP_TRUNC({{ dbt_date.from_unixtimestamp("min(ts)", format="milliseconds") }}, HOUR) AS bucket_ingestion_datetime,
    userid AS user_id,
    firstname AS first_name,
    lastname AS last_name,
    gender,
    COALESCE(JSON_VALUE(TO_JSON_STRING(t), '$.level'), 'free') AS level,
    {{ 
        dbt_date.from_unixtimestamp("registration", format="milliseconds") 
    }} AS registration_datetime
FROM
    {{ source(env_var('DBT_SOURCE_DATASET'), 'user_info') }} AS t
{% if is_incremental() %}

    WHERE {{ dbt_date.from_unixtimestamp("ts", format="milliseconds") }} >= TIMESTAMP_SUB(TIMESTAMP("{{ max_ingestion_datetime }}"), INTERVAL 10 MINUTE)
        AND {{ dbt_date.from_unixtimestamp("ts", format="milliseconds") }} > (
            SELECT COALESCE(MAX(this_table.event_datetime), "1900-01-01") - INTERVAL 10 MINUTE
            FROM {{ this }} AS this_table
        )

{% endif %}
-- Use groupby to handle cases where source application emits same user data multiple times
GROUP BY 3, 4, 5, 6, 7, 8
