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
    MIN(date) AS bucket_ingestion_datetime,
    userid AS user_id,
    firstname AS first_name,
    lastname AS last_name,
    gender,
    level,
    {{ 
        dbt_date.from_unixtimestamp("registration", format="milliseconds") 
    }} AS registration_datetime
FROM
    {{ source(env_var('DBT_BIGQUERY_DATASET'), 'users_ext') }}
{% if is_incremental() %}

    WHERE date >= "{{ max_ingestion_datetime }}"
        AND {{ dbt_date.from_unixtimestamp("ts", format="milliseconds") }} > (
            SELECT COALESCE(MAX(this_table.event_datetime), "1900-01-01") - INTERVAL 10 MINUTE
            FROM {{ this }} AS this_table
        )

{% endif %}
-- Use groupby to handle cases where source application emits same user data multiple times
GROUP BY 3, 4, 5, 6, 7, 8
