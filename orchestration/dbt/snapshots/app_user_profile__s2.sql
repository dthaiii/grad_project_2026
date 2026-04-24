{% snapshot app_user_profile__s2 %}

{{ config(
    target_schema='ods',
    unique_key='user_id',
    strategy='check',
    check_cols=['first_name', 'last_name', 'gender', 'level'],
    invalidate_hard_deletes=True,
    post_hook=[
      "update {{ this }} set tf_record_status = 0, tf_updated_at = coalesce(dbt_valid_to, current_timestamp()) where dbt_valid_to is not null and tf_record_status != 0"
    ]
) }}

SELECT
    user_id,
    first_name,
    last_name,
    gender,
    level,
    registration_datetime,
    CURRENT_TIMESTAMP() AS tf_created_at,
    TIMESTAMP('3000-01-01') AS tf_updated_at,
    CURRENT_TIMESTAMP() AS tf_etl_at,
    1 AS tf_record_status
FROM {{ ref('stg_users__fu') }}

{% endsnapshot %}
