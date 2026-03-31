{{ config(
    materialized='incremental',
    unique_key = ['event_datetime', 'pk_user', 'pk_song'],
    partition_by={
      "field": "event_datetime",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}

{% if is_incremental() %}
    {%- set max_event_datetime =  get_value_from_query(
        "COALESCE(MAX(event_datetime),'1900-01-01') - INTERVAL 10 MINUTE", this
        ) -%}
{% endif %}

SELECT
    listen.event_datetime,
    users.pk_user,
    locations.pk_location,
    songs.pk_song
FROM
    {{ ref('stg_listen_events') }} AS listen
LEFT JOIN {{ ref('dim_users') }} AS users
    ON listen.user_id = users.user_id
        AND listen.event_datetime >= users.row_effective_datetime
        AND listen.event_datetime < users.row_expiry_datetime
LEFT JOIN {{ ref('dim_locations') }} AS locations
    ON listen.city = locations.city
        AND listen.state = locations.state_code
        AND listen.postal_code = locations.raw_postal_code
LEFT JOIN {{ ref('dim_songs') }} AS songs
    ON listen.song = songs.title
        AND listen.artist = songs.artist
        AND listen.duration = songs.duration
{% if is_incremental() %}

    WHERE listen.event_datetime > "{{ max_event_datetime }}"

{% endif %}
