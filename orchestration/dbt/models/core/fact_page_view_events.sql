{{ config(
    materialized='table'
) }}

WITH src_page_view_events AS (
    SELECT
        ts,
        sessionid,
        page,
        auth,
        artist,
        song,
        duration,
        userid,
        firstname,
        lastname,
        gender,
        zip,
        level
    FROM {{ source('data_staging', 'page_view_events') }}
),

stg_user_info AS (
    SELECT
        CAST(user_id AS INT64) AS user_id,
        first_name,
        last_name,
        gender,
        CAST(zip AS STRING) AS zip,
        level
    FROM {{ ref('stg_user_info') }}
),

enriched AS (
    SELECT
        pve.ts,
        pve.sessionid AS sessionId,
        pve.page,
        pve.auth,
        pve.artist,
        pve.song,
        pve.duration,
        CAST(pve.userid AS INT64) AS userId,
        COALESCE(pve.firstname, ui.first_name) AS firstName,
        COALESCE(pve.lastname, ui.last_name) AS lastName,
        COALESCE(pve.gender, ui.gender) AS gender,
        COALESCE(CAST(pve.zip AS STRING), ui.zip) AS zip,
        COALESCE(pve.level, ui.level) AS level,
        CASE
            WHEN pve.page = 'NextSong' AND COALESCE(pve.level, ui.level) = 'free' THEN 0.005
            ELSE 0
        END AS ad_revenue
    FROM src_page_view_events AS pve
    LEFT JOIN stg_user_info AS ui
        ON CAST(pve.userid AS INT64) = ui.user_id
)

SELECT
    ts,
    sessionId,
    page,
    auth,
    artist,
    song,
    duration,
    userId,
    firstName,
    lastName,
    gender,
    zip,
    level,
    ad_revenue
FROM enriched
