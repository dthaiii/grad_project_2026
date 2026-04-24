{{ config(
    materialized='table',
    partition_by={
      "field": "session_start_datetime",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by = ["pk_user", "pk_location", "pk_entry_page"]
) }}

WITH dedup_page_view_events AS (
    SELECT
        pve.*
    FROM {{ ref('stg_page_view_events') }} AS pve
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            pve.event_datetime,
            pve.user_id,
            pve.session_id,
            pve.item_in_session,
            pve.page
        ORDER BY pve.event_datetime
    ) = 1
),

location_exact AS (
    SELECT
        UPPER(TRIM(city)) AS city_norm,
        UPPER(TRIM(state_code)) AS state_norm,
        CAST(raw_postal_code AS STRING) AS raw_postal_code,
        MIN(pk_location) AS pk_location
    FROM {{ ref('dim_locations') }}
    GROUP BY 1, 2, 3
),

location_fallback AS (
    SELECT
        UPPER(TRIM(city)) AS city_norm,
        UPPER(TRIM(state_code)) AS state_norm,
        MIN(pk_location) AS pk_location
    FROM {{ ref('dim_locations') }}
    GROUP BY 1, 2
),

user_fallback AS (
    SELECT
        user_id,
        MIN(pk_user) AS pk_user
    FROM {{ ref('dim_users') }}
    GROUP BY 1
),

session_facts_no_first_page AS (
    SELECT
        session_id,
        user_id,
        postal_code,
        city,
        state,
        level,
        MIN(item_in_session) AS first_item_index,
        COUNT(*) AS num_pages_visited,
        MIN(event_datetime) AS session_start_datetime,
        {{ dbt.datediff("MIN(event_datetime)", "MAX(event_datetime)", "SECOND") }} 
            AS session_seconds,
        SUM(CASE WHEN ad_revenue > 0 THEN 1 ELSE 0 END) AS num_ads_served,
        SUM(ad_revenue) AS total_ad_revenue
    FROM
        dedup_page_view_events
    
    GROUP BY
        1, 2, 3, 4, 5, 6
),

session_facts AS (
    SELECT
        snf.session_id,
        snf.user_id,
        snf.postal_code,
        snf.city,
        snf.state,
        snf.level,
        pve.page AS first_page,
        snf.num_pages_visited,
        snf.session_start_datetime,
        snf.session_seconds,
        snf.num_ads_served,
        snf.total_ad_revenue
    FROM
        session_facts_no_first_page AS snf
    INNER JOIN
        dedup_page_view_events AS pve
        ON snf.session_id = pve.session_id
            AND snf.user_id = pve.user_id
            AND snf.postal_code IS NOT DISTINCT FROM pve.postal_code
            AND snf.city = pve.city
            AND snf.state = pve.state
            AND snf.level = pve.level
            AND snf.first_item_index = pve.item_in_session
),

fact_session_candidates AS (
    SELECT
        session_facts.session_start_datetime,
        session_facts.session_id,
        COALESCE(users.pk_user, user_fallback.pk_user) AS pk_user,
        COALESCE(loc_exact.pk_location, loc_fallback.pk_location) AS pk_location,
        pages.pk_page AS pk_entry_page,
        session_facts.session_seconds,
        session_facts.num_pages_visited,
        session_facts.num_ads_served,
        session_facts.total_ad_revenue,
        ROW_NUMBER() OVER (
            PARTITION BY
                session_facts.session_start_datetime,
                session_facts.session_id,
                session_facts.user_id
            ORDER BY users.row_effective_datetime DESC
        ) AS rn
    FROM
        session_facts
    LEFT JOIN {{ ref('dim_users') }} AS users
        ON session_facts.user_id = users.user_id
            AND session_facts.session_start_datetime >= users.row_effective_datetime
            AND session_facts.session_start_datetime < users.row_expiry_datetime
    LEFT JOIN user_fallback
        ON session_facts.user_id = user_fallback.user_id
    LEFT JOIN location_exact AS loc_exact
        ON UPPER(TRIM(session_facts.city)) = loc_exact.city_norm
            AND UPPER(TRIM(session_facts.state)) = loc_exact.state_norm
            AND session_facts.postal_code = loc_exact.raw_postal_code
    LEFT JOIN location_fallback AS loc_fallback
        ON UPPER(TRIM(session_facts.city)) = loc_fallback.city_norm
            AND UPPER(TRIM(session_facts.state)) = loc_fallback.state_norm
    LEFT JOIN {{ ref('dim_pages') }} AS pages
        ON session_facts.first_page = pages.page_name
)

SELECT
    session_start_datetime,
    session_id,
    pk_user,
    pk_location,
    pk_entry_page,
    session_seconds,
    num_pages_visited,
    num_ads_served,
    total_ad_revenue
FROM fact_session_candidates
WHERE rn = 1
