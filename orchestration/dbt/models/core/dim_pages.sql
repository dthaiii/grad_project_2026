{{ config(materialized='table') }}

WITH distinct_pages AS (
    SELECT DISTINCT page AS page_name
    FROM
        {{ ref('stg_page_view_events') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(["page_name"]) }} AS pk_page,
    page_name,
    {{ get_page_category("page_name") }} AS page_category
FROM
    distinct_pages
