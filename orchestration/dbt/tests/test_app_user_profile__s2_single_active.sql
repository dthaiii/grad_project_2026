SELECT
  user_id
FROM {{ ref('app_user_profile__s2') }}
WHERE tf_record_status = 1
GROUP BY user_id
HAVING COUNT(*) > 1
