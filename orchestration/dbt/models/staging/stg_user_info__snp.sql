{{ config(materialized='table') }}

SELECT
    userid AS user_id,
    firstname AS first_name,
    lastname AS last_name,
    gender,
    COALESCE(level, 'free') AS level,
    {{ dbt_date.from_unixtimestamp("ts", format="milliseconds") }} AS tf_sourcing_at,
    CURRENT_TIMESTAMP() AS tf_etl_at
FROM {{ source('data_staging', 'user_info') }}
