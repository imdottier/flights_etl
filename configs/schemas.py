from pyspark.sql.types import (
    StructField, StructType,
    StringType, ArrayType, DoubleType, BooleanType
)

# Reusable sub-schemas to avoid repetition

# Schema for objects containing UTC and Local timestamps
timestamp_schema = StructType([
    StructField("utc", StringType()),
    StructField("local", StringType())
])

# Schema for the nested location object within an airport
airport_location_schema = StructType([
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType())
])

# Schema for the airport information block
airport_schema = StructType([
    StructField("icao", StringType()),
    StructField("iata", StringType()),
    StructField("localCode", StringType()),
    StructField("name", StringType()),
    StructField("shortName", StringType()),
    StructField("municipalityName", StringType()),
    StructField("location", airport_location_schema),
    StructField("countryCode", StringType()),
    StructField("timeZone", StringType())
])

# Schema for the main departure or arrival information block
movement_schema = StructType([
    StructField("airport", airport_schema),
    StructField("scheduledTime", timestamp_schema),
    StructField("revisedTime", timestamp_schema),
    StructField("predictedTime", timestamp_schema),
    StructField("runwayTime", timestamp_schema),
    StructField("terminal", StringType()),
    StructField("checkInDesk", StringType()),
    StructField("gate", StringType()),
    StructField("baggageBelt", StringType()),
    StructField("runway", StringType()),
    StructField("quality", ArrayType(StringType()))
])

# Schema for the aircraft image details
image_schema = StructType([
    StructField("url", StringType()),
    StructField("webUrl", StringType()),
    StructField("author", StringType()),
    StructField("title", StringType()),
    StructField("description", StringType()),
    StructField("license", StringType()),
    StructField("htmlAttributions", ArrayType(StringType()))
])

# Schema for the aircraft information block
aircraft_schema = StructType([
    StructField("reg", StringType()),
    StructField("modeS", StringType()),
    StructField("model", StringType()),
    StructField("image", image_schema)
])

# Schema for the airline information block
airline_schema = StructType([
    StructField("name", StringType()),
    StructField("iata", StringType()),
    StructField("icao", StringType())
])

# Schemas for the various measurement units in the location block
distance_schema = StructType([
    StructField("meter", DoubleType()),
    StructField("km", DoubleType()),
    StructField("mile", DoubleType()),
    StructField("nm", DoubleType()),
    StructField("feet", DoubleType())
])

pressure_schema = StructType([
    StructField("hPa", DoubleType()),
    StructField("inHg", DoubleType()),
    StructField("mmHg", DoubleType())
])

speed_schema = StructType([
    StructField("kt", DoubleType()),
    StructField("kmPerHour", DoubleType()),
    StructField("miPerHour", DoubleType()),
    StructField("meterPerSecond", DoubleType())
])

track_schema = StructType([
    StructField("deg", DoubleType()),
    StructField("rad", DoubleType())
])

# Schema for the live location information block
location_schema = StructType([
    StructField("pressureAltitude", distance_schema),
    StructField("altitude", distance_schema),
    StructField("pressure", pressure_schema),
    StructField("groundSpeed", speed_schema),
    StructField("trueTrack", track_schema),
    StructField("reportedAtUtc", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType())
])

# Schema for a single flight record (used in both departures and arrivals arrays)
flight_record_schema = StructType([
    StructField("departure", movement_schema),
    StructField("arrival", movement_schema),
    StructField("number", StringType()),
    StructField("callSign", StringType()),
    StructField("status", StringType()),
    StructField("codeshareStatus", StringType()),
    StructField("isCargo", BooleanType()),
    StructField("aircraft", aircraft_schema),
    StructField("airline", airline_schema),
    StructField("location", location_schema)
])

# The final, top-level schema for the entire API response
flights_schema = StructType([
    StructField("departures", ArrayType(flight_record_schema)),
    StructField("arrivals", ArrayType(flight_record_schema))
])


# flights_schema = StructType([
#     StructField("departures", ArrayType(
#         StructType([
#             StructField("departure", StructType([
#                 StructField("scheduledTime", StructType([
#                     StructField("utc", TimestampType(), True),
#                     StructField("local", TimestampType(), True)
#                 ]), True),
#                 StructField("revisedTime", StructType([
#                     StructField("utc", TimestampType(), True),
#                     StructField("local", TimestampType(), True)
#                 ]), True),
#                 StructField("checkInDesk", StringType(), True),
#                 StructField("gate", StringType(), True),
#                 StructField("quality", ArrayType(StringType()), True),
#             ])),

#             StructField("arrival", StructType([
#                 StructField("airport", StructType([
#                     StructField("icao", StringType(), True),
#                     StructField("iata", StringType(), True),
#                     StructField("name", StringType(), True),
#                     StructField("timeZone", StringType(), True),
#                 ]), True),
#                 StructField("scheduledTime", StructType([
#                     StructField("utc", TimestampType(), True),
#                     StructField("local", TimestampType(), True)
#                 ]), True),
#                 StructField("revisedTime", StructType([
#                     StructField("utc", TimestampType(), True),
#                     StructField("local", TimestampType(), True)
#                 ]), True),
#                 StructField("runwayTime", StructType([
#                     StructField("utc", TimestampType(), True),
#                     StructField("local", TimestampType(), True)
#                 ]), True),
#                 StructField("terminal", StringType(), True),
#                 StructField("baggageBelt", StringType(), True),
#                 StructField("runway", StringType(), True),
#                 StructField("gate", StringType(), True),
#                 StructField("quality", ArrayType(StringType()), True),
#             ])),

#             StructField("number", StringType(), True),
#             StructField("status", StringType(), True),
#             StructField("codeshareStatus", StringType(), True),
#             StructField("isCargo", BooleanType(), True),
#             StructField("aircraft", StructType([
#                 StructField("model", StringType(), True),
#             ]), True),
#             StructField("airline", StructType([
#                 StructField("name", StringType(), True),
#                 StructField("iata", StringType(), True),
#                 StructField("icao", StringType(), True),
#             ]), True),      
#         ])
#     ))
# ])


# Country schema
country_schema = StructType([
    StructField("code", StringType()),
    StructField("name", StringType())
])

# Continent schema
continent_schema = StructType([
    StructField("code", StringType()),
    StructField("name", StringType())
])

# URLs schema
urls_schema = StructType([
    StructField("webSite", StringType()),
    StructField("wikipedia", StringType()),
    StructField("twitter", StringType()),
    StructField("liveAtc", StringType()),
    StructField("flightRadar", StringType()),
    StructField("googleMaps", StringType())
])

# Runway schema (nested)
runway_schema = StructType([
    StructField("name", StringType()),
    StructField("trueHdg", DoubleType()),
    StructField("length", distance_schema),
    StructField("width", distance_schema),
    StructField("isClosed", BooleanType()),
    StructField("location", airport_location_schema),
    StructField("surface", StringType()),
    StructField("displacedThreshold", distance_schema),
    StructField("hasLighting", BooleanType())
])

# CurrentTime schema
current_time_schema = StructType([
    StructField("time", timestamp_schema),
    StructField("timeZoneId", StringType())
])


full_airport_schema = StructType([
    StructField("icao", StringType()),
    StructField("iata", StringType()),
    StructField("localCode", StringType()),
    StructField("shortName", StringType()),
    StructField("fullName", StringType()),
    StructField("municipalityName", StringType()),
    StructField("location", airport_location_schema),
    StructField("elevation", distance_schema),
    StructField("country", country_schema),
    StructField("continent", continent_schema),
    StructField("timeZone", StringType()),
    StructField("urls", urls_schema),
    StructField("runways", ArrayType(runway_schema)),
    StructField("currentTime", current_time_schema)
])
