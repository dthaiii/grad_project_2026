{{ config(materialized='table') }}

WITH distinct_songs AS (
    SELECT DISTINCT
        artist,
        {{ normalize_song_name('title') }} AS title,
        ROUND(duration, 2) AS duration,
        genre
    FROM
        {{ source(env_var('DBT_BIGQUERY_DATASET'), 'songs_ext') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(["artist", "title", "duration"]) }}
        AS pk_song,
    *
FROM distinct_songs
