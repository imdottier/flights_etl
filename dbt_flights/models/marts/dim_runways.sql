{{ config(
    materialized='incremental',
    unique_key='runway_version_bk'    
) }}


WITH runways AS (
    SELECT * FROM {{ source('staging', 'runways') }}
),

cleaned_runways AS (
    SELECT
        r.*,
        CASE
            WHEN surface LIKE '%-%' OR surface LIKE '%/%' THEN 'MIXED'
            WHEN surface LIKE 'ASP%' THEN 'ASP'
            WHEN surface LIKE 'CON%' THEN 'CON'
            WHEN surface LIKE 'TURF%' THEN 'TURF'
            WHEN surface LIKE 'GRS%' OR surface LIKE 'GRVL%' OR surface LIKE 'GRAVEL%' THEN 'GRS'
            WHEN surface LIKE 'GRE%' THEN 'GRE'
            WHEN surface LIKE 'BIT%' THEN 'BIT'
            WHEN surface LIKE 'MAC%' THEN 'MAC'
            WHEN surface LIKE 'WATER%' THEN 'WATER'
            WHEN surface LIKE 'SAN%' THEN 'SAN'
            WHEN surface LIKE 'SNOW%' THEN 'SNOW'
            ELSE 'UNK'
        END AS surface_type_code
    FROM runways AS r
),

unknown_runway AS (
    SELECT
        encode(digest('-1', 'sha256'), 'hex') AS runway_version_bk,
        encode(digest('-1', 'sha256'), 'hex') AS runway_bk,
        encode(digest('-1', 'sha256'), 'hex') AS airport_bk,
        'UNK' AS runway_name,
        0.0 AS true_heading,
        'Unknown' AS surface,
        FALSE AS has_lighting,
        FALSE AS is_closed,
        0.0 AS length_feet,
        0.0 AS width_feet,
        0.0 AS displaced_threshold_feet,
        0.0 AS latitude,
        0.0 AS longitude,
        0.0 AS elevation_feet,
        NULL AS runway_end_type,
        '1970-01-01'::DATE AS effective_start_date,
        '9999-12-31'::DATE AS effective_end_date,
        NULL::timestamp AS _ingested_at,
        NULL::timestamp AS _inserted_at
),

-- Just so I can SELECT once and not twice
runways_for_union AS (
    SELECT
        runway_version_bk,
        runway_bk,
        airport_bk,
        runway_name,
        true_heading,
        surface_type_code AS surface,
        has_lighting,
        is_closed,
        length_feet,
        width_feet,
        displaced_threshold_feet,
        latitude,
        longitude,
        elevation_feet,
        runway_end_type,
        effective_start_date,
        effective_end_date,
        _ingested_at,
        _inserted_at

    FROM cleaned_runways
)

SELECT * FROM runways_for_union
UNION ALL
SELECT * FROM unknown_runway