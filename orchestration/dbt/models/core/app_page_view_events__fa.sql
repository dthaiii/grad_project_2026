{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    on_schema_change='append_new_columns',
    partition_by={
      "field": "event_datetime",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}

SELECT
    stg.event_datetime,
    stg.session_id,
    stg.user_id,
    stg.page,
    stg.ad_revenue,
    stg.level,
    COALESCE(profile.first_name, 'NA') AS first_name,
    COALESCE(profile.last_name, 'NA') AS last_name,
    COALESCE(profile.gender, 'NA') AS gender,
    COALESCE(stg.postal_code, profile.zip) AS postal_code,
    stg.tf_sourcing_at,
    CURRENT_TIMESTAMP() AS tf_etl_at
FROM {{ ref('stg_page_view_events__fa') }} AS stg
LEFT JOIN {{ ref('stg_user_profile__snp') }} AS profile
    ON stg.user_id = profile.user_id
WHERE 1 = 1
{% if is_incremental() %}
  AND stg.event_datetime > (
      SELECT COALESCE(MAX(event_datetime), TIMESTAMP('1900-01-01'))
      FROM {{ this }}
  )
{% endif %}
