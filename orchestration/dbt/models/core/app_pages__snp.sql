{{ config(materialized='table') }}

WITH distinct_pages AS (
    SELECT DISTINCT
        page AS page_name,
        MIN(tf_sourcing_at) AS tf_sourcing_at
    FROM {{ ref('stg_page_view_events__fa') }}
    GROUP BY 1
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['page_name']) }} AS pk_page,
    page_name,
    {{ get_page_category('page_name') }} AS page_category,
    tf_sourcing_at,
    CURRENT_TIMESTAMP() AS tf_etl_at
FROM distinct_pages
