{{ config(
    materialized='table',
    cluster_by=['user_id', 'tf_record_status']
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['CAST(user_id AS STRING)', 'CAST(dbt_valid_from AS STRING)']) }} AS pk_user,
    user_id,
    first_name,
    last_name,
    gender,
    registration_datetime,
    level,
    CASE 
        WHEN ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY dbt_valid_from) = 1 
        THEN TIMESTAMP('1900-01-01')
        ELSE dbt_valid_from 
    END AS row_effective_datetime,
    COALESCE(dbt_valid_to, TIMESTAMP('3000-01-01')) AS row_expiry_datetime,
    tf_created_at,
    tf_updated_at,
    tf_etl_at,
    tf_record_status
FROM {{ ref('app_user_profile__s2') }}
