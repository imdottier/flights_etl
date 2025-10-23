WITH intermediate_flights AS (
    SELECT * FROM {{ source('gold_layer', 'fct_flights_intermediate') }}
),

dim_flight_details AS (
    SELECT * FROM {{ source('gold_layer', 'dim_flight_details') }}
),

base AS (
    SELECT
        f.*,

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


SELECT
    -- Pass-through Keys (assuming fact_flight_id is NOT in the intermediate table)
    f.flight_composite_pk,
    f.flight_details_sk,
    f.departure_airport_sk,
    f.arrival_airport_sk,
    f.airline_sk,
    f.aircraft_sk,
    f.departure_runway_sk,
    f.arrival_runway_sk,
    f.quality_combo_sk,

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
        WHEN delay_arrival_minutes <= 15 THEN TRUE
        ELSE FALSE
    END AS is_on_time,

    CASE
        WHEN delay_departure_minutes > 15 AND delay_arrival_minutes <= 15 THEN TRUE
        ELSE FALSE
    END AS made_up_time_in_air,

    CASE
        WHEN fd.flight_status = 'Canceled' THEN TRUE
        ELSE FALSE
    END AS is_cancelled,
    
    -- Metadata
    f.ingestion_hour

FROM
    base AS f

LEFT JOIN
    dim_flight_details AS fd
    ON f.flight_details_sk = fd.flight_details_sk
