INSERT INTO gold.dim_airports (
    airport_sk, airport_iata, airport_icao, airport_name,
    municipality_name, country_name, continent_name,
    latitude, longitude, elevation_feet, airport_time_zone)
VALUES (
    -1, 'UNK', 'UNK', 'Unknown',
    'Unknown', 'Unknown', 'Unknown',
    0, 0, 0, 'Unknown'
)
ON CONFLICT (airport_sk) DO NOTHING;