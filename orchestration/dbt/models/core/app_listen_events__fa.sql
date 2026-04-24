{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    partition_by={
      "field": "event_datetime",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=['pk_user', 'pk_song', 'pk_location']
) }}

WITH source_events AS (
    SELECT
        event_datetime,
        user_id,
        postal_code,
        city,
        state,
        song,
        artist,
        duration,
        tf_sourcing_at
    FROM {{ ref('stg_listen_events__fa') }}
    {% if is_incremental() %}
    WHERE event_datetime > (
        SELECT COALESCE(MAX(event_datetime), TIMESTAMP('1900-01-01'))
        FROM {{ this }}
    )
    {% endif %}
),

song_match AS (
    SELECT
        s.*,
        songs.pk_song,
        ROW_NUMBER() OVER (
          PARTITION BY s.event_datetime, s.user_id, s.song, s.artist, CAST(s.duration AS STRING), s.postal_code, s.city, s.state
          ORDER BY ABS(COALESCE(s.duration, 0.0) - COALESCE(songs.duration, 0.0)), songs.pk_song
        ) AS rn
    FROM source_events AS s
    LEFT JOIN {{ ref('app_songs__snp') }} AS songs
      ON REPLACE(s.song, '\\', '') = REPLACE(songs.title, '\\', '')
     AND LOWER(TRIM(s.artist)) = LOWER(TRIM(songs.artist))
),

joined AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
          'CAST(m.event_datetime AS STRING)',
          'CAST(m.user_id AS STRING)',
          'm.song',
          'm.artist',
          'CAST(m.duration AS STRING)',
          'm.postal_code',
          'm.city',
          'm.state'
        ]) }} AS listen_event_key,
        m.event_datetime,
        users.pk_user,
        loc.pk_location,
        m.pk_song,
        m.tf_sourcing_at,
        CURRENT_TIMESTAMP() AS tf_etl_at
    FROM song_match AS m
    LEFT JOIN {{ ref('app_users__s2') }} AS users
      ON m.user_id = users.user_id
     AND m.event_datetime >= users.row_effective_datetime
     AND m.event_datetime < users.row_expiry_datetime
    LEFT JOIN {{ ref('app_locations__snp') }} AS loc
      ON m.postal_code = loc.raw_postal_code
     AND UPPER(TRIM(m.city)) = UPPER(TRIM(loc.city))
     AND UPPER(TRIM(m.state)) = UPPER(TRIM(loc.state_code))
    WHERE m.rn = 1
)

SELECT * FROM joined
