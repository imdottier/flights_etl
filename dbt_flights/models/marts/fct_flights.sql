{{ config(
    materialized='incremental',
    unique_key='flight_composite_pk'    
) }}


WITH flights AS (
    SELECT *
    FROM {{ source('staging', 'flights') }}
),

airports AS (
    SELECT * FROM {{ ref('dim_airports') }}
),

airlines AS (
    SELECT * FROM {{ source('staging', 'airlines') }}
),

runways AS (
    SELECT * FROM {{ source('staging', 'runways') }}
),

aircrafts AS (
    SELECT * FROM {{ source('staging', 'aircrafts') }}
),

regions AS (
    SELECT * FROM {{ ref('dim_regions') }}
),

-- merge on business keys isnt perfect
-- so we do this to make sure left join doesnt produce more rows
lookup AS (
    WITH dr_one AS (
        SELECT *
        FROM (
            SELECT
                dr.*,
                ROW_NUMBER() OVER (
                    PARTITION BY airport_bk, runway_name
                    ORDER BY _ingested_at DESC 
                ) AS rn
            FROM runways dr
            WHERE dr.effective_end_date IS NULL
        ) t
        WHERE rn = 1
    ),
    ar_one AS (
        SELECT *
        FROM (
            SELECT
                ar.*,
                ROW_NUMBER() OVER (
                    PARTITION BY airport_bk, runway_name
                    ORDER BY _ingested_at DESC
                ) AS rn
            FROM runways ar
            WHERE ar.effective_end_date IS NULL
        ) t
        WHERE rn = 1
    )

    SELECT
        f.*,
        dr_one.runway_version_bk AS departure_runway_version_bk_resolved,
        ar_one.runway_version_bk AS arrival_runway_version_bk_resolved
    FROM flights AS f
    LEFT JOIN dr_one
        ON f.departure_airport_bk = dr_one.airport_bk
       AND f.dep_runway_name = dr_one.runway_name
    LEFT JOIN ar_one
        ON f.arrival_airport_bk = ar_one.airport_bk
       AND f.arr_runway_name = ar_one.runway_name
),

-- Handle derived cols
base AS (
    SELECT
        f.*,
        dap.airport_iata AS departure_airport_iata_code,
        dap.airport_icao AS departure_airport_icao_code,
        dap.airport_name AS departure_airport_name,
        dap.iso_region AS departure_iso_region,
        dr.region_name AS departure_region_name,
        dap.iso_country AS departure_iso_country,
        dap.country_name AS departure_country_name,
        dr.continent_code AS departure_continent_code,
        
        aap.airport_iata AS arrival_airport_iata_code,
        aap.airport_icao AS arrival_airport_icao_code,
        aap.airport_name AS arrival_airport_name,
        aap.iso_region AS arrival_iso_region,
        ar.region_name AS arrival_region_name,
        aap.iso_country AS arrival_iso_country,
        aap.country_name AS arrival_country_name,
        ar.continent_code AS arrival_continent_code,

        TO_CHAR(f.dep_scheduled_at_utc, 'YYYYMMDD')::INT AS departure_date_key,
        TO_CHAR(f.arr_scheduled_at_utc, 'YYYYMMDD')::INT AS arrival_date_key,
        EXTRACT(HOUR FROM dep_scheduled_at_utc) AS dep_hour_utc,
        EXTRACT(DOW FROM dep_scheduled_at_utc) AS dep_day_of_week,
        EXTRACT(HOUR FROM arr_scheduled_at_utc) AS arr_hour_utc,
        EXTRACT(DOW FROM arr_scheduled_at_utc) AS arr_day_of_week,

        -- === DERIVED MEASURES & FLAGS ===
        -- Durations (Robust against NULLs)
        CASE
            WHEN f.arr_scheduled_at_utc IS NOT NULL AND f.dep_scheduled_at_utc IS NOT NULL THEN
                (EXTRACT(EPOCH FROM (f.arr_scheduled_at_utc - f.dep_scheduled_at_utc))) / 60
            ELSE NULL
        END AS scheduled_duration_minutes,
        
        CASE
            WHEN f.arr_runway_at_utc IS NOT NULL AND f.dep_runway_at_utc IS NOT NULL THEN
                (EXTRACT(EPOCH FROM (f.arr_runway_at_utc - f.dep_runway_at_utc))) / 60
            ELSE NULL
        END AS actual_duration_minutes,

        -- Delays (Robust against NULLs) - IMPORTANT: Define these first to reuse them
        CASE
            WHEN f.dep_runway_at_utc IS NOT NULL AND f.dep_scheduled_at_utc IS NOT NULL THEN
                (EXTRACT(EPOCH FROM (f.dep_runway_at_utc - f.dep_scheduled_at_utc))) / 60
            ELSE NULL
        END AS delay_departure_minutes,
        
        CASE
            WHEN f.arr_runway_at_utc IS NOT NULL AND f.arr_scheduled_at_utc IS NOT NULL THEN
                (EXTRACT(EPOCH FROM (f.arr_runway_at_utc - f.arr_scheduled_at_utc))) / 60
            ELSE NULL
        END AS delay_arrival_minutes,

        -- Taxi Times
        CASE
            WHEN f.dep_runway_at_utc IS NOT NULL AND f.dep_revised_at_utc IS NOT NULL THEN
                (EXTRACT(EPOCH FROM (f.dep_runway_at_utc - f.dep_revised_at_utc))) / 60
            ELSE NULL
        END AS taxi_out_minutes,

        -- Lat and lon of departure and arrival airports for estimation
        dap.latitude * PI() / 180 AS dep_lat,
        dap.longitude * PI() / 180 AS dep_lon,
        aap.latitude * PI() / 180 AS arr_lat,
        aap.longitude * PI() / 180 AS arr_lon

    FROM
        lookup AS f

    LEFT JOIN
        airports AS dap
        ON f.departure_airport_bk = dap.airport_bk
    LEFT JOIN
        airports AS aap
        ON f.arrival_airport_bk = aap.airport_bk
    LEFT JOIN
        regions AS dr
        ON dap.iso_region = dr.region_code
    LEFT JOIN
        regions AS ar
        ON aap.iso_region = ar.region_code

),

haversine_a AS (
    -- Compute 'a' in the Haversine formula
    SELECT
        f.*,
        SIN((arr_lat - dep_lat) / 2) * SIN((arr_lat - dep_lat) / 2)
        + COS(dep_lat) * COS(arr_lat)
        * SIN((arr_lon - dep_lon) / 2) * SIN((arr_lon - dep_lon) / 2) AS a
    FROM base AS f
),

haversine_c AS (
    -- Compute 'c' = 2 * atan2(sqrt(a), sqrt(1 - a))
    SELECT
        f.*,
        2 * ATAN2(SQRT(a), SQRT(1 - a)) AS c
    FROM haversine_a AS f
),

final_distance AS (
    -- Compute distance in kilometers
    SELECT
        f.*,
        ROUND((6371 * c)::numeric, 2) AS distance_km
    FROM haversine_c AS f
)

-- Final Schema with joined dimensions
SELECT
    -- Pass-through Keys (assuming fact_flight_id is NOT in the intermediate table)
    f.flight_composite_pk,
    -- Planned to partition by flight_date but Postgres is funny so no
    f.flight_date,
    
    f.departure_airport_bk,
    f.arrival_airport_bk,
    f.airline_bk,
    f.aircraft_bk,
    COALESCE(
        f.departure_runway_version_bk,
        f.departure_runway_version_bk_resolved,
        encode(digest('-1', 'sha256'), 'hex')
    ) AS departure_runway_version_bk,
    COALESCE(
        f.arrival_runway_version_bk,
        f.arrival_runway_version_bk_resolved,
        encode(digest('-1', 'sha256'), 'hex')
    ) AS arrival_runway_version_bk,
    f.departure_date_key,
    f.arrival_date_key,

    -- Denormalized Dimensions
    f.departure_airport_iata_code,
    f.departure_airport_icao_code,
    f.departure_airport_name,
    f.departure_iso_region,
    f.departure_region_name,
    f.departure_iso_country,
    f.departure_country_name,
    f.departure_continent_code,
    
    f.arrival_airport_iata_code,
    f.arrival_airport_icao_code,
    f.arrival_airport_name,
    f.arrival_iso_region,
    f.arrival_region_name,
    f.arrival_iso_country,
    f.arrival_country_name,
    f.arrival_continent_code,

    dal.airline_iata AS airline_iata_code,
    dal.airline_icao AS airline_icao_code,
    dal.airline_name AS airline_name,
    dac.aircraft_reg AS aircraft_reg,
    dac.aircraft_mode_s AS aircraft_mode_s,
    dr.runway_name AS departure_runway_name,
    ar.runway_name AS arrival_runway_name,

    -- Pass-through Degenerate Dimensions
    f.flight_number,
    f.flight_callsign,

    f.flight_status,
    f.codeshare_status,
    f.is_cargo,
    f.dep_terminal,
    f.arr_gate,
    f.dep_checkin_desk,
    f.arr_terminal,
    f.arr_baggage_belt,
    f.dep_local_timezone,
    f.arr_local_timezone,
    f.dep_has_basic,
    f.dep_has_live,
    f.arr_has_basic,
    f.arr_has_live,
    f.quality_desc,
    
    -- Pass-through Raw Measures
    f.dep_scheduled_at_utc,
    f.dep_revised_at_utc,
    f.dep_runway_at_utc,
    f.arr_scheduled_at_utc,
    f.arr_revised_at_utc,
    f.arr_runway_at_utc,
    f.last_location_reported_at_utc,
    f.latitude,
    f.longitude,
    f.altitude_ft,
    f.ground_speed_kts,
    f.true_track_deg,

    f.scheduled_duration_minutes,
    f.actual_duration_minutes,
    f.delay_departure_minutes,
    f.delay_arrival_minutes,
    f.taxi_out_minutes,

    -- === DERIVED MEASURES & FLAGS ===
    f.distance_km,
    CASE 
        WHEN actual_duration_minutes > 0
            THEN ROUND(((f.distance_km / actual_duration_minutes) * 60)::numeric, 2)
        ELSE NULL
    END AS avg_speed_kmh,

    f.actual_duration_minutes - f.scheduled_duration_minutes AS schedule_variance_minutes,
    CASE
        WHEN delay_arrival_minutes <= 15 THEN TRUE      -- The condition is met
        WHEN delay_arrival_minutes > 15 THEN FALSE      -- The condition is not met
        ELSE NULL                                        -- The data to evaluate the condition is missing
    END AS is_on_time,

    CASE
        WHEN delay_departure_minutes IS NULL THEN NULL
        WHEN delay_departure_minutes <= 0 THEN 'Early'
        WHEN delay_departure_minutes <= 15 THEN 'On Time'
        WHEN delay_departure_minutes <= 60 THEN 'Minor Delay'
        ELSE 'Major Delay'
    END AS departure_delay_category,

    CASE
        WHEN delay_arrival_minutes IS NULL THEN NULL
        WHEN delay_arrival_minutes <= 0 THEN 'Early'
        WHEN delay_arrival_minutes <= 15 THEN 'On Time'
        WHEN delay_arrival_minutes <= 60 THEN 'Minor Delay'
        ELSE 'Major Delay'
    END AS arrival_delay_category,

    CASE
        WHEN f.distance_km IS NULL THEN NULL
        WHEN f.distance_km < 500 THEN 'Short-Haul'
        WHEN f.distance_km < 2000 THEN 'Medium-Haul'
        ELSE 'Long-Haul'
    END AS flight_type,

    CASE
        WHEN delay_departure_minutes > 15 AND delay_arrival_minutes <= 15 THEN TRUE
        WHEN NOT (delay_departure_minutes > 15 AND delay_arrival_minutes <= 15) THEN FALSE
        ELSE NULL
    END AS made_up_time_in_air,

    CASE
        WHEN flight_status = 'Canceled' THEN TRUE
        ELSE FALSE
    END AS is_cancelled,
    
    -- Metadata
    f._ingested_at,
    f._inserted_at,
    f.ingestion_hour

FROM
    final_distance AS f

LEFT JOIN
    airlines AS dal
    ON f.airline_bk = dal.airline_bk
LEFT JOIN
    aircrafts AS dac
    ON f.aircraft_bk = dac.aircraft_bk
LEFT JOIN 
    runways AS dr
    ON f.departure_runway_version_bk = dr.runway_version_bk
LEFT JOIN 
    runways AS ar
    ON f.arrival_runway_version_bk = ar.runway_version_bk
WHERE
    actual_duration_minutes > 0
    OR actual_duration_minutes IS NULL

-- flight_date
-- flight_composite_pk
-- departure_airport_bk
-- arrival_airport_bk
-- airline_bk
-- aircraft_bk
-- departure_runway_version_bk
-- arrival_runway_version_bk
-- flight_number
-- flight_callsign
-- flight_status
-- codeshare_status
-- is_cargo
-- dep_terminal
-- arr_gate
-- dep_checkin_desk
-- dep_gate
-- arr_terminal
-- arr_baggage_belt
-- dep_local_timezone
-- arr_local_timezone
-- dep_has_basic
-- dep_has_live
-- arr_has_basic
-- arr_has_live
-- quality_desc
-- dep_scheduled_at_utc
-- dep_revised_at_utc
-- dep_runway_at_utc
-- arr_scheduled_at_utc
-- arr_revised_at_utc
-- arr_runway_at_utc
-- last_location_reported_at_utc
-- latitude
-- longitude
-- altitude_ft
-- ground_speed_kts
-- true_track_deg
-- ingestion_hour
-- _ingested_at
-- _inserted_at
