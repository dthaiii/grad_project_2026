{{ config(materialized='table') }}

SELECT
    user_id,
    first_name,
    last_name,
    gender,
    level,
    registration_datetime,
    row_effective_datetime,
    row_expiry_datetime,
    tf_created_at,
    tf_updated_at,
    tf_etl_at
FROM {{ ref('app_users__s2') }}
WHERE tf_record_status = 1
