{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    on_schema_change='append_new_columns',
    partition_by={
      "field": "event_datetime",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=['pk_user', 'pk_location']
) }}

WITH source_events AS (
    SELECT
        event_datetime,
        user_id,
        session_id,
        postal_code,
        city,
        state,
        level,
        success,
        tf_sourcing_at
    FROM {{ ref('stg_auth_events__fa') }}
    {% if is_incremental() %}
    WHERE event_datetime > (
        SELECT COALESCE(MAX(event_datetime), TIMESTAMP('1900-01-01'))
        FROM {{ this }}
    )
    {% endif %}
),

joined AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
          'CAST(m.event_datetime AS STRING)',
          'CAST(m.user_id AS STRING)',
          'CAST(m.session_id AS STRING)',
          'm.postal_code',
          'm.city',
          'm.state'
        ]) }} AS auth_event_key,
        m.event_datetime,
        users.pk_user,
        m.session_id,
        loc.pk_location,
        m.level,
        m.success,
        m.tf_sourcing_at,
        CURRENT_TIMESTAMP() AS tf_etl_at
    FROM source_events AS m
    LEFT JOIN {{ ref('app_users__s2') }} AS users
      ON m.user_id = users.user_id
     AND m.event_datetime >= users.row_effective_datetime
     AND m.event_datetime < users.row_expiry_datetime
    LEFT JOIN (
        SELECT city, state_code, MIN(pk_location) as pk_location
        FROM {{ ref('app_locations__snp') }}
        GROUP BY 1, 2
    ) AS loc
      ON UPPER(TRIM(m.city)) = UPPER(TRIM(loc.city))
     AND UPPER(TRIM(m.state)) = UPPER(TRIM(loc.state_code))
)

SELECT * FROM joined
