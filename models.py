# models.py
# This file defines the complete SQLAlchemy schema for the data warehouse.
# Alembic uses this file as the "source of truth" to generate migrations.

from sqlalchemy import (
    Boolean, Column, DateTime, Double, ForeignKey, Integer, String, Text,
    CHAR, func, Date, BigInteger
)
from sqlalchemy.orm import declarative_base

# The declarative base is the foundation for all models.
Base = declarative_base()


class DimRegions(Base):
    __tablename__ = 'dim_regions'
    
    region_code = Column(String(128), primary_key=True)
    region_name = Column(String(255), nullable=True)
    local_code = Column(String(50), nullable=True)
    continent_code = Column(String(2), nullable=True)
    iso_country = Column(String(2), nullable=True)
    country_name = Column(String(255), nullable=True)
    _ingested_at = Column(DateTime, nullable=True)
    _inserted_at = Column(DateTime, nullable=False, server_default=func.now())


class DimAirports(Base):
    __tablename__ = 'dim_airports'
    
    airport_bk = Column(CHAR(64), primary_key=True)
    iso_region = Column(String(128), ForeignKey('dim_regions.region_code'), nullable=True)
    iso_country = Column(String(2), nullable=True)
    airport_iata = Column(String(10))
    airport_icao = Column(String(10))
    airport_gps = Column(String(255), nullable=True)
    airport_local_code = Column(String(50), nullable=True)
    airport_name = Column(String(255))
    municipality_name = Column(String(255))
    country_name = Column(String(255))
    continent_name = Column(String(255))
    latitude = Column(Double)
    longitude = Column(Double)
    elevation_feet = Column(Double)
    airport_type = Column(String(50), nullable=True)
    scheduled_service = Column(String(10), nullable=True)
    airport_time_zone = Column(String)
    _ingested_at = Column(DateTime)
    _inserted_at = Column(DateTime, server_default=func.now())


class DimRunways(Base):
    __tablename__ = 'dim_runways'
    
    runway_version_bk = Column(Text, primary_key=True)
    runway_bk = Column(Text, nullable=True)
    airport_bk = Column(CHAR(64), ForeignKey('dim_airports.airport_bk'), nullable=False)
    runway_name = Column(String)
    true_heading = Column(Double, nullable=True)
    surface = Column(String)
    has_lighting = Column(Boolean)
    is_closed = Column(Boolean)
    length_feet = Column(Double)
    width_feet = Column(Double)
    displaced_threshold_feet = Column(Double, nullable=True)
    latitude = Column(Double, nullable=True)
    longitude = Column(Double, nullable=True)
    elevation_feet = Column(Double, nullable=True)
    runway_end_type = Column(String, nullable=True)
    effective_start_date = Column(DateTime)
    effective_end_date = Column(DateTime)
    _ingested_at = Column(DateTime)
    _inserted_at = Column(DateTime, server_default=func.now())


class DimAirlines(Base):
    __tablename__ = 'dim_airlines'

    airline_bk = Column(Text, primary_key=True)
    airline_iata = Column(String(10))
    airline_icao = Column(String(10))
    airline_name = Column(String(255))
    _ingested_at = Column(DateTime)
    _inserted_at = Column(DateTime, server_default=func.now())


class DimAircrafts(Base):
    __tablename__ = 'dim_aircrafts'

    aircraft_bk = Column(Text, primary_key=True)
    aircraft_reg = Column(String(50))
    aircraft_mode_s = Column(String(50))
    aircraft_model = Column(String(255))
    aircraft_bk_type = Column(String(255)) # As per your schema
    _ingested_at = Column(DateTime)
    _inserted_at = Column(DateTime, server_default=func.now())


class DimQualityCombination(Base):
    __tablename__ = 'dim_quality_combination'

    quality_combo_sk = Column(Integer, primary_key=True)
    dep_has_basic = Column(Boolean)
    dep_has_live = Column(Boolean)
    arr_has_basic = Column(Boolean)
    arr_has_live = Column(Boolean)
    quality_desc = Column(Text)
    _ingested_at = Column(DateTime)
    _inserted_at = Column(DateTime, server_default=func.now())


class DimFlightDetails(Base):
    __tablename__ = 'dim_flight_details'

    flight_details_sk = Column(Integer, primary_key=True)
    flight_status = Column(Text)
    codeshare_status = Column(Text)
    is_cargo = Column(Boolean)
    _ingested_at = Column(DateTime)
    _inserted_at = Column(DateTime, server_default=func.now())


class FctFlightsIntermediate(Base):
    __tablename__ = 'fct_flights_intermediate'

    fact_flight_id = Column(BigInteger, primary_key=True)
    flight_composite_pk = Column(Text, unique=True, nullable=False)
    
    # Foreign Keys
    flight_details_sk = Column(Integer, ForeignKey('dim_flight_details.flight_details_sk'))
    # IMPORTANT: The type must match the primary key it references (CHAR(64))
    departure_airport_bk = Column(CHAR(64), ForeignKey('dim_airports.airport_bk'))
    arrival_airport_bk = Column(CHAR(64), ForeignKey('dim_airports.airport_bk'))
    airline_bk = Column(Text, ForeignKey('dim_airlines.airline_bk'))
    aircraft_bk = Column(Text, ForeignKey('dim_aircrafts.aircraft_bk'))
    departure_runway_version_bk = Column(Text, ForeignKey('dim_runways.runway_version_bk'))
    arrival_runway_version_bk = Column(Text, ForeignKey('dim_runways.runway_version_bk'))
    quality_combo_sk = Column(Integer, ForeignKey('dim_quality_combination.quality_combo_sk'))
    
    # Degenerate Dimensions
    flight_number = Column(Text)
    flight_callsign = Column(Text, nullable=True)
    
    # Other Attributes
    dep_terminal = Column(Text, nullable=True)
    arr_gate = Column(Text, nullable=True)
    dep_checkin_desk = Column(Text, nullable=True)
    dep_gate = Column(Text, nullable=True)
    arr_terminal = Column(Text, nullable=True)
    arr_baggage_belt = Column(Text, nullable=True)
    dep_local_timezone = Column(Text, nullable=True)
    arr_local_timezone = Column(Text, nullable=True)

    # Timestamps
    dep_scheduled_at_utc = Column(DateTime)
    dep_revised_at_utc = Column(DateTime, nullable=True)
    dep_runway_at_utc = Column(DateTime, nullable=True)
    arr_scheduled_at_utc = Column(DateTime)
    arr_revised_at_utc = Column(DateTime, nullable=True)
    arr_runway_at_utc = Column(DateTime, nullable=True)
    last_location_reported_at_utc = Column(DateTime, nullable=True)

    # Measures
    latitude = Column(Double, nullable=True)
    longitude = Column(Double, nullable=True)
    altitude_ft = Column(Double, nullable=True)
    ground_speed_kts = Column(Double, nullable=True)
    true_track_deg = Column(Double, nullable=True)
    
    # Partitioning/Audit Columns
    ingestion_hour = Column(Text)
    flight_date = Column(Date)
    _ingested_at = Column(DateTime)
    _inserted_at = Column(DateTime, server_default=func.now())

    __table_args__ = {
        'postgresql_partition_by': 'RANGE (flight_date)'
    }


class DwhWatermarks(Base):
    __tablename__ = '_dwh_watermarks'
    # This special table lives in a different schema.
    __table_args__ = {'schema': 'dwh_meta'}

    table_name = Column(String(255), primary_key=True)
    last_inserted_at = Column(DateTime)