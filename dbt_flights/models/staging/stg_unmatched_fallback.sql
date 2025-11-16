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
),

non_matching_airports AS (
    SELECT * FROM airports_joined
    WHERE r_region_code IS NULL
),

-- Join the non-matching to regions on iso_country or country_name
fallback_join AS (
    SELECT
        a.*,
        r2.region_code AS r2_region_code,
        r2.iso_country AS r2_iso_country, 
        r2.country_name AS r2_country_name
    FROM non_matching_airports a
    LEFT JOIN regions r2
        ON a.iso_country = r2.iso_country
        OR a.country_name = r2.country_name
)

SELECT
    f.*,
    CONCAT_WS('|', 'UNK', COALESCE(iso_country, country_name)) AS iso_region_resolved
FROM fallback_join f
WHERE r2_region_code IS NULL
