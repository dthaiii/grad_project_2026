{{ config(
    materialized='table',
    partition_by={
      "field": "activity_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=["artist", "song"]
) }}

WITH base AS (
    SELECT
        activity_date,
        song,
        artist,
        play_count,
        unique_listeners,
        rank
    FROM {{ ref('mart_daily_trending_songs') }}
),

daily_totals AS (
    SELECT
        activity_date,
        SUM(play_count) AS total_daily_plays,
        SUM(unique_listeners) AS total_daily_unique_listeners
    FROM base
    GROUP BY 1
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'CAST(base.activity_date AS STRING)',
        'base.song',
        'base.artist'
    ]) }} AS song_daily_fact_key,
    base.activity_date,
    base.song,
    base.artist,
    base.play_count,
    base.unique_listeners,
    base.rank AS daily_rank,
    totals.total_daily_plays,
    totals.total_daily_unique_listeners,
    ROUND(SAFE_DIVIDE(base.play_count, NULLIF(totals.total_daily_plays, 0)), 4) AS play_share_pct,
    CASE WHEN base.rank <= 10 THEN TRUE ELSE FALSE END AS is_top_10_song
FROM base
LEFT JOIN daily_totals AS totals
    ON base.activity_date = totals.activity_date
