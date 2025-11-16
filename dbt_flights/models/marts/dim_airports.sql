{{ config(
    materialized='incremental',
    unique_key='airport_bk'    
) }}


-- Thought a macro could help, end up with this mess
{% set all_cols = [
    'airport_bk',
    'iso_region',
    'iso_country',
    'airport_iata',
    'airport_icao',
    'airport_gps',
    'airport_local_code',
    'airport_name',
    'municipality_name',
    'country_name',
    'continent_name',
    'latitude',
    'longitude',
    'elevation_feet',
    'airport_type',
    'scheduled_service',
    'airport_time_zone',
    '_ingested_at',
    '_inserted_at',

    'r_region_code',
    'r_iso_country',
    'r_country_name',

    'r2_region_code',
    'r2_iso_country',
    'r2_country_name',

    'iso_region_resolved',
    'iso_country_resolved',
    'country_name_resolved',

    'iso_region_fallback'
] %}

{% set table_cols = {
    'matching_airports': [
        'airport_bk', 'iso_region', 'iso_country', 'airport_iata', 'airport_icao',
        'airport_gps', 'airport_local_code', 'airport_name', 'municipality_name',
        'country_name', 'continent_name', 'latitude', 'longitude', 'elevation_feet',
        'airport_type', 'scheduled_service', 'airport_time_zone', '_ingested_at',
        '_inserted_at', 'iso_region_fallback', 'r_region_code', 'r_iso_country', 
        'r_country_name', 'iso_region_resolved', 'iso_country_resolved', 'country_name_resolved'
    ],
    'matched_fallback': [
        'airport_bk', 'iso_region', 'iso_country', 'airport_iata', 'airport_icao',
        'airport_gps', 'airport_local_code', 'airport_name', 'municipality_name',
        'country_name', 'continent_name', 'latitude', 'longitude', 'elevation_feet',
        'airport_type', 'scheduled_service', 'airport_time_zone', '_ingested_at',
        '_inserted_at', 'iso_region_fallback', 'r_region_code', 'r_iso_country',
        'r_country_name', 'r2_region_code', 'r2_iso_country', 'r2_country_name',
        'iso_region_resolved', 'iso_country_resolved', 'country_name_resolved'
    ],
    'unmatched_fallback': [
        'airport_bk', 'iso_region', 'iso_country', 'airport_iata', 'airport_icao',
        'airport_gps', 'airport_local_code', 'airport_name', 'municipality_name',
        'country_name', 'continent_name', 'latitude', 'longitude', 'elevation_feet',
        'airport_type', 'scheduled_service', 'airport_time_zone', '_ingested_at',
        '_inserted_at', 'iso_region_fallback', 'r_region_code', 'r_iso_country',
        'r_country_name', 'r2_region_code', 'r2_iso_country', 'r2_country_name',
        'iso_region_resolved'
    ],
    'unknown_airport': [
        'airport_bk', 'airport_iata', 'airport_icao', 'airport_gps', 'airport_local_code',
        'airport_name', 'municipality_name', 'iso_region', 'iso_country', 'country_name',
        'continent_name', 'latitude', 'longitude', 'elevation_feet', 'airport_type',
        'scheduled_service', 'airport_time_zone'
    ]
} %}

WITH matching_airports AS (
    SELECT * FROM {{ ref('stg_matching_airports') }}
),

matched_fallback AS (
    SELECT * FROM {{ ref('stg_matched_fallback') }}
),

unmatched_fallback AS (
    SELECT * FROM {{ ref('stg_unmatched_fallback') }}
),

unknown_airport AS (
    SELECT
        encode(digest('-1', 'sha256'), 'hex') AS airport_bk,
        'UNK' AS airport_iata,
        'UNK' AS airport_icao,
        NULL AS airport_gps,
        NULL AS airport_local_code,
        'Unknown' AS airport_name,
        'Unknown' AS municipality_name,
        'ZZ-U-A' AS iso_region, -- code for unknown region
        'Unknown' AS iso_country,
        'Unknown' AS country_name,
        'Unknown' AS continent_name,
        0.0 AS latitude,
        0.0 AS longitude,
        0.0 AS elevation_feet,
        'Unknown' AS airport_type,
        FALSE AS scheduled_service,
        'Unknown' AS airport_time_zone
),

union_airports AS (
  {{ union_with_nulls(table_cols, all_cols) }}
)

SELECT
    airport_bk,

    COALESCE(iso_region_resolved, iso_region) AS iso_region,
    COALESCE(iso_country_resolved, iso_country) AS iso_country,
    COALESCE(country_name_resolved, country_name) AS country_name,
    airport_iata,
    airport_icao,
    airport_gps,
    airport_local_code,
    airport_name,
    municipality_name,

    latitude,
    longitude,
    elevation_feet,
    airport_type,
    scheduled_service,
    airport_time_zone,
    _ingested_at,
    _inserted_at

FROM union_airports