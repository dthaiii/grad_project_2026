{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='user_id',
    on_schema_change='sync_all_columns'
) }}

WITH source_users AS (
    SELECT
        userid AS user_id,
        firstname AS first_name,
        lastname AS last_name,
        gender,
        COALESCE(level, 'free') AS level,
        {{ dbt_date.from_unixtimestamp("registration", format="milliseconds") }} AS registration_datetime,
        {{ dbt_date.from_unixtimestamp("ts", format="milliseconds") }} AS tf_sourcing_at
    FROM {{ source('data_staging', 'user_info') }}
),

dedup_users AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY tf_sourcing_at DESC) AS rn
    FROM source_users
),

latest_users AS (
    SELECT
        user_id,
        first_name,
        last_name,
        gender,
        level,
        registration_datetime,
        tf_sourcing_at
    FROM dedup_users
    WHERE rn = 1
)

{% if is_incremental() %}
,existing AS (
    SELECT user_id, tf_created_at
    FROM {{ this }}
)
{% endif %}

SELECT
    src.user_id,
    src.first_name,
    src.last_name,
    src.gender,
    src.level,
    src.registration_datetime,
    src.tf_sourcing_at,
    {% if is_incremental() %}
    COALESCE(ex.tf_created_at, CURRENT_TIMESTAMP()) AS tf_created_at,
    {% else %}
    CURRENT_TIMESTAMP() AS tf_created_at,
    {% endif %}
    CURRENT_TIMESTAMP() AS tf_updated_at,
    CURRENT_TIMESTAMP() AS tf_etl_at,
    1 AS tf_record_status
FROM latest_users AS src
{% if is_incremental() %}
LEFT JOIN existing AS ex ON src.user_id = ex.user_id
{% endif %}
