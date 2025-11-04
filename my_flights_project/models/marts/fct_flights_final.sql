{{
    config(
        materialized='incremental',
        unique_key='fact_flight_id',
        post_hook=[
            "INSERT INTO {{ source('dwh_meta', '_dwh_watermarks') }} (table_name, last_inserted_at)
            VALUES (
                '{{ this.name }}',
                (SELECT MAX(_inserted_at) FROM {{ this }})
            )
            ON CONFLICT (table_name) DO UPDATE
            SET last_inserted_at = EXCLUDED.last_inserted_at;"
        ]
    )
}}

WITH watermarks AS (
    SELECT * FROM {{ source('dwh_meta', '_dwh_watermarks') }}
),

intermediate_flights AS (
    SELECT *
    FROM {{ source('gold_layer', 'fct_flights_intermediate') }}
    
    {% if is_incremental() %}

    WHERE
        _inserted_at > (
            SELECT COALESCE(last_inserted_at, '1970-01-01'::TIMESTAMP)
            FROM watermarks
            WHERE table_name = '{{ this.name }}'
        )

    {% endif %}
),

dim_airports AS (
    SELECT * FROM {{ source('gold_layer', 'dim_airports') }}
),

dim_flight_details AS (
    SELECT * FROM {{ source('gold_layer', 'dim_flight_details') }}
),

dim_airlines AS (
    SELECT * FROM {{ source('gold_layer', 'dim_airlines') }}
),

dim_runways AS (
    SELECT * FROM {{ source('gold_layer', 'dim_runways') }}
),

dim_aircrafts AS (
    SELECT * FROM {{ source('gold_layer', 'dim_aircrafts') }}
),

dim_quality_combination AS (
    SELECT * FROM {{ source('gold_layer', 'dim_quality_combination') }}
),

-- Handle derived cols
base AS (
    SELECT
        f.*,

        TO_CHAR(f.dep_scheduled_at_utc, 'YYYYMMDD')::INT AS departure_date_key,
        TO_CHAR(f.arr_scheduled_at_utc, 'YYYYMMDD')::INT AS arrival_date_key,

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
        END AS taxi_out_minutes
        
    FROM
        intermediate_flights AS f
)


-- Final Schema with joined dimensions
SELECT
    -- Pass-through Keys (assuming fact_flight_id is NOT in the intermediate table)
    f.fact_flight_id,
    f.flight_composite_pk,
    f.flight_details_sk,
    f.departure_airport_sk,
    f.arrival_airport_sk,
    f.airline_sk,
    f.aircraft_sk,
    f.departure_runway_version_key,
    f.arrival_runway_version_key,
    f.quality_combo_sk,
    f.departure_date_key,
    f.arrival_date_key,

    -- Denormalized Dimensions
    dap.airport_iata AS departure_airport_iata_code,
    dap.airport_icao AS departure_airport_icao_code,
    dap.airport_name AS departure_airport_name,
    aap.airport_iata AS arrival_airport_iata_code,
    aap.airport_icao AS arrival_airport_icao_code,
    aap.airport_name AS arrival_airport_name,
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
    f.dep_terminal,
    f.arr_gate,
    f.dep_checkin_desk,
    f.arr_terminal,
    f.arr_baggage_belt,
    f.dep_local_timezone,
    f.arr_local_timezone,
    
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
    f.actual_duration_minutes - f.scheduled_duration_minutes AS schedule_variance_minutes,
    CASE
        WHEN delay_arrival_minutes <= 15 THEN TRUE      -- The condition is met
        WHEN delay_arrival_minutes > 15 THEN FALSE      -- The condition is not met
        ELSE NULL                                        -- The data to evaluate the condition is missing
    END AS is_on_time,

    CASE
        WHEN delay_departure_minutes > 15 AND delay_arrival_minutes <= 15 THEN TRUE
        WHEN NOT (delay_departure_minutes > 15 AND delay_arrival_minutes <= 15) THEN FALSE
        ELSE NULL
    END AS made_up_time_in_air,

    CASE
        WHEN fd.flight_status = 'Canceled' THEN TRUE
        ELSE FALSE
    END AS is_cancelled,
    
    -- Metadata
    f._ingested_at,
    f._inserted_at,
    f.ingestion_hour

FROM
    base AS f

LEFT JOIN
    dim_airports AS dap
    ON f.departure_airport_sk = dap.airport_sk
LEFT JOIN
    dim_airports AS aap
    ON f.arrival_airport_sk = aap.airport_sk
LEFT JOIN
    dim_airlines AS dal
    ON f.airline_sk = dal.airline_sk
LEFT JOIN
    dim_aircrafts AS dac
    ON f.aircraft_sk = dac.aircraft_sk
LEFT JOIN 
    dim_runways AS dr
    ON f.departure_runway_version_key = dr.runway_version_key
LEFT JOIN 
    dim_runways AS ar
    ON f.arrival_runway_version_key = ar.runway_version_key
LEFT JOIN 
    dim_quality_combination AS dq
    ON f.quality_combo_sk = dq.quality_combo_sk
LEFT JOIN
    dim_flight_details AS fd
    ON f.flight_details_sk = fd.flight_details_sk