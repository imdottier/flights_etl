{{ config(
    materialized='incremental',
    unique_key='region_code'    
) }}

WITH regions AS (
    SELECT * FROM {{ source('staging', 'regions') }}
),
unmatched_fallback_regions AS (
    SELECT * FROM {{ ref('stg_unmatched_fallback_regions') }}
),
union_regions AS (
    SELECT
        region_code,
        region_name,
        local_code,
        continent_code,
        iso_country,
        country_name,
        _ingested_at,
        _inserted_at,
        NULL AS iso_region_resolved,
        NULL AS iso_country_resolved,
        NULL AS country_name_resolved
    FROM regions
    
    UNION ALL
    
    SELECT
        NULL AS region_code,
        NULL AS region_name,
        NULL AS local_code,
        NULL AS continent_code,
        NULL AS iso_country,
        NULL AS country_name,
        ingested_at AS _ingested_at,
        inserted_at AS _inserted_at,
        iso_region_resolved,
        iso_country_resolved,
        country_name_resolved
    FROM unmatched_fallback_regions
)
SELECT
    COALESCE(region_code, iso_region_resolved) AS region_code,
    region_name,
    local_code,
    continent_code,
    COALESCE(iso_country, iso_country_resolved) AS iso_country,
    COALESCE(country_name, country_name_resolved) AS country_name,
    _ingested_at,
    _inserted_at
FROM union_regions