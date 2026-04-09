{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='listen_event_key',
    on_schema_change='sync_all_columns',
    partition_by={
      "field": "event_datetime",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by = ["pk_user", "pk_song", "pk_location"]
) }}

WITH dedup_listen_events AS (
    SELECT
        listen.*
    FROM {{ ref('stg_listen_events') }} AS listen
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            listen.event_datetime,
            listen.user_id,
            listen.postal_code,
            listen.city,
            listen.state,
            listen.song,
            listen.artist,
            CAST(listen.duration AS STRING)
        ORDER BY listen.event_datetime
    ) = 1
),

song_match_candidates AS (
    SELECT
        listen.event_datetime,
        listen.user_id,
        listen.postal_code,
        listen.city,
        listen.state,
        listen.song,
        listen.artist,
        listen.duration,
        songs.pk_song,
        ROW_NUMBER() OVER (
            PARTITION BY
                listen.event_datetime,
                listen.user_id,
                listen.song,
                listen.artist,
                CAST(listen.duration AS STRING),
                listen.postal_code,
                listen.city,
                listen.state
            ORDER BY
                ABS(COALESCE(listen.duration, 0.0) - COALESCE(songs.duration, 0.0)),
                songs.pk_song
        ) AS rn_song
    FROM dedup_listen_events AS listen
    LEFT JOIN {{ ref('dim_songs') }} AS songs
        ON REPLACE(listen.song, '\\', '') = REPLACE(songs.title, '\\', '')
            AND LOWER(TRIM(listen.artist)) = LOWER(TRIM(songs.artist))
            AND ABS(COALESCE(listen.duration, 0.0) - COALESCE(songs.duration, 0.0)) <= 0.01
),

best_song_match AS (
    SELECT
        event_datetime,
        user_id,
        postal_code,
        city,
        state,
        song,
        artist,
        duration,
        pk_song
    FROM song_match_candidates
    WHERE rn_song = 1
),

fact_listen_candidates AS (
    SELECT
        CAST(listen.event_datetime AS TIMESTAMP) AS event_datetime,
        listen.user_id,
        listen.postal_code,
        listen.city,
        listen.state,
        listen.song,
        listen.artist,
        listen.duration,
        users.pk_user,
        locations.pk_location,
        listen.pk_song,
        ROW_NUMBER() OVER (
            PARTITION BY
                listen.event_datetime,
                listen.user_id,
                listen.song,
                listen.artist,
                CAST(listen.duration AS STRING),
                listen.postal_code,
                listen.city,
                listen.state
            ORDER BY users.row_effective_datetime DESC
        ) AS rn
    FROM
        best_song_match AS listen
    LEFT JOIN {{ ref('dim_users') }} AS users
        ON listen.user_id = users.user_id
            AND listen.event_datetime >= users.row_effective_datetime
            AND listen.event_datetime < users.row_expiry_datetime
    LEFT JOIN {{ ref('dim_locations') }} AS locations
        ON listen.city = locations.city
            AND listen.state = locations.state_code
            AND listen.postal_code = locations.raw_postal_code
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'CAST(event_datetime AS STRING)',
        'CAST(user_id AS STRING)',
        'song',
        'artist',
        'CAST(duration AS STRING)',
        'postal_code',
        'city',
        'state'
    ]) }} AS listen_event_key,
    event_datetime,
    pk_user,
    pk_location,
    pk_song
FROM fact_listen_candidates
WHERE rn = 1
