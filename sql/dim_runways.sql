INSERT INTO gold.dim_runways (
    runway_sk, airport_sk, runway_name, true_heading, surface, has_lighting,
    is_closed, length_feet, width_feet, displaced_threshold_feet,
    latitude, longitude, effective_start_date, effective_end_date
)
VALUES (
    -1, -1, 'UNK', 0.0, 'Unknown', False,
    False, 0.0, 0.0, 0.0,
    0.0, 0.0, '1970-01-01', '9999-12-31'
)
ON CONFLICT (runway_sk) DO NOTHING;