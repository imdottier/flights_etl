{{ config(materialized='view') }}

WITH airports AS (
    SELECT * FROM {{ source('staging', 'airports') }}
),

regions AS (
    SELECT * FROM {{ source('staging', 'regions') }}
),

modified_airports AS (
    SELECT
        a.*,
        CASE 
            WHEN iso_region IS NOT NULL
                OR iso_country IS NOT NULL
                OR country_name IS NOT NULL
                THEN iso_region
            ELSE 'ZZ-U-A'
        END AS iso_region_fallback
    FROM airports a
),

airports_joined AS (
    SELECT
        a.*,
        r.region_code AS r_region_code,
        r.iso_country AS r_iso_country,
        r.country_name AS r_country_name
    FROM modified_airports a
    LEFT JOIN regions r
    ON a.iso_region_fallback = r.region_code
)

SELECT 
    a.*,
    r_region_code AS iso_region_resolved,
    r_iso_country AS iso_country_resolved,
    r_country_name AS country_name_resolved
FROM airports_joined a
WHERE r_region_code IS NOT NULL
