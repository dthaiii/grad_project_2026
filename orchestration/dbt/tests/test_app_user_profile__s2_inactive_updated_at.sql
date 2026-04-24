SELECT *
FROM {{ ref('app_user_profile__s2') }}
WHERE tf_record_status = 0
  AND tf_updated_at = TIMESTAMP('3000-01-01')
