{{ config(
    materialized='table',
    partition_by={
      "field": "session_start_datetime",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}

WITH session_facts_no_first_page AS (
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
        SUM(CASE page WHEN 'PlayAd' THEN 1 ELSE 0 END) AS num_ads_served,
        SUM(ad_revenue) AS total_ad_revenue
    FROM
        {{ ref('stg_page_view_events') }}
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
        {{ ref('stg_page_view_events') }} AS pve
        ON snf.session_id = pve.session_id
            AND snf.user_id = pve.user_id
            AND snf.postal_code = pve.postal_code
            AND snf.city = pve.city
            AND snf.state = pve.state
            AND snf.level = pve.level
            AND snf.first_item_index = pve.item_in_session
)

SELECT
    session_facts.session_start_datetime,
    session_facts.session_id,
    users.pk_user,
    locations.pk_location,
    pages.pk_page AS pk_entry_page,
    session_facts.session_seconds,
    session_facts.num_pages_visited,
    session_facts.num_ads_served,
    session_facts.total_ad_revenue
FROM
    session_facts
LEFT JOIN {{ ref('dim_users') }} AS users
    ON session_facts.user_id = users.user_id
        AND session_facts.session_start_datetime >= users.row_effective_datetime
        AND session_facts.session_start_datetime < users.row_expiry_datetime
LEFT JOIN {{ ref('dim_locations') }} AS locations
    ON session_facts.city = locations.city
        AND session_facts.state = locations.state_code
        AND session_facts.postal_code = locations.raw_postal_code
LEFT JOIN {{ ref('dim_pages') }} AS pages
    ON session_facts.first_page = pages.page_name
