{{ config(
    materialized='table',
    partition_by={
      "field": "session_start_datetime",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=['pk_user', 'pk_location', 'pk_entry_page']
) }}

WITH dedup_page_view_events AS (
    SELECT
        pve.*
    FROM {{ ref('stg_page_view_events__fa') }} AS pve
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY pve.event_datetime, pve.user_id, pve.session_id, pve.item_in_session, pve.page
        ORDER BY pve.event_datetime
    ) = 1
),

session_facts AS (
    SELECT
        session_id,
        user_id,
        postal_code,
        city,
        state,
        MIN(item_in_session) AS first_item_index,
        MIN(event_datetime) AS session_start_datetime,
        MAX(event_datetime) AS session_end_datetime,
        COUNT(*) AS num_pages_visited,
        SUM(CASE WHEN ad_revenue > 0 THEN 1 ELSE 0 END) AS num_ads_served,
        SUM(ad_revenue) AS total_ad_revenue
    FROM dedup_page_view_events
    GROUP BY 1, 2, 3, 4, 5
),

entry_page AS (
    SELECT
        sf.session_id,
        sf.user_id,
        sf.postal_code,
        sf.city,
        sf.state,
        sf.session_start_datetime,
        sf.session_end_datetime,
        sf.num_pages_visited,
        sf.num_ads_served,
        sf.total_ad_revenue,
        pve.page AS first_page
    FROM session_facts AS sf
    JOIN dedup_page_view_events AS pve
      ON sf.session_id = pve.session_id
     AND sf.user_id = pve.user_id
     AND sf.first_item_index = pve.item_in_session
),

final_join AS (
    SELECT
        ep.session_start_datetime,
        ep.session_id,
        users.pk_user,
        loc.pk_location,
        pages.pk_page AS pk_entry_page,
        TIMESTAMP_DIFF(ep.session_end_datetime, ep.session_start_datetime, SECOND) AS session_seconds,
        ep.num_pages_visited,
        ep.num_ads_served,
        ep.total_ad_revenue,
        ep.session_start_datetime AS tf_sourcing_at,
        CURRENT_TIMESTAMP() AS tf_etl_at
    FROM entry_page AS ep
    LEFT JOIN {{ ref('app_users__s2') }} AS users
      ON ep.user_id = users.user_id
     AND ep.session_start_datetime >= users.row_effective_datetime
     AND ep.session_start_datetime < users.row_expiry_datetime
    LEFT JOIN {{ ref('app_locations__snp') }} AS loc
      ON ep.postal_code = loc.raw_postal_code
     AND UPPER(TRIM(ep.city)) = UPPER(TRIM(loc.city))
     AND UPPER(TRIM(ep.state)) = UPPER(TRIM(loc.state_code))
    LEFT JOIN {{ ref('app_pages__snp') }} AS pages
      ON ep.first_page = pages.page_name
)

SELECT * FROM final_join
