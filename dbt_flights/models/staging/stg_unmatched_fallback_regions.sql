{{ config(materialized='view') }}

WITH airports AS (
    SELECT * FROM {{ source('staging', 'airports') }}
),

regions AS (
    SELECT * FROM {{ source('staging', 'regions') }}
),

joined_airports AS (
    SELECT
        r.*,
        a.iso_country AS a_iso_country,
        a.country_name AS a_country_name

    FROM airports a
    
    LEFT JOIN regions r
        ON a.iso_region = r.region_code
        OR a.iso_country = r.iso_country
        OR a.country_name = r.country_name

    -- if all null then they will be set to the unknown region
    WHERE a.iso_region IS NOT NULL
       OR a.iso_country IS NOT NULL
       OR a.country_name IS NOT NULL
)

SELECT
    CONCAT_WS('|', 'UNK', COALESCE(a_iso_country, a_country_name)) AS iso_region_resolved,
    a_iso_country AS iso_country_resolved,
    a_country_name AS country_name_resolved,    
    CURRENT_TIMESTAMP::timestamp AS ingested_at,
    CURRENT_TIMESTAMP::timestamp AS inserted_at

FROM joined_airports a
WHERE region_code IS NULL