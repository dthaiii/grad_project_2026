{{ config(materialized='table') }}

WITH distinct_songs AS (
    SELECT DISTINCT
        artist,
        {{ normalize_song_name('title') }} AS title,
        ROUND(duration, 2) AS duration,
        genre
    FROM {{ source('data_staging', 'songs_ext') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['artist', 'title', 'duration']) }} AS pk_song,
    artist,
    title,
    duration,
    genre,
    CURRENT_TIMESTAMP() AS tf_sourcing_at,
    CURRENT_TIMESTAMP() AS tf_etl_at
FROM distinct_songs
