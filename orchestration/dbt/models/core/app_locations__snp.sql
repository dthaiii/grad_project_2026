{{ config(materialized='table') }}

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['country_code', 'postal_code', 'city', 'state']) }} AS pk_location,
    country_code,
    {{ standardise_postal_code('postal_code', 'country_code') }} AS standardised_postal_code,
    CAST(postal_code AS STRING) AS raw_postal_code,
    COALESCE(city, 'NO CITY') AS city,
    COALESCE(state, 'NO STATE') AS state,
    CAST(state_code AS STRING) AS state_code,
    CURRENT_TIMESTAMP() AS tf_sourcing_at,
    CURRENT_TIMESTAMP() AS tf_etl_at
FROM {{ source('data_staging', 'locations_ext') }}
