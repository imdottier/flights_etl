"""create gold schema

Revision ID: e97a607a0ec0
Revises: 
Create Date: 2025-10-21 16:23:43.047580

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e97a607a0ec0'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute("""
        CREATE SCHEMA IF NOT EXISTS gold;
               
        -- dimension tables
        CREATE TABLE IF NOT EXISTS gold.dim_airports (
            airport_sk BIGINT PRIMARY KEY,
            airport_iata VARCHAR(10),
            airport_icao VARCHAR(10),
            airport_name VARCHAR(255),
            municipality_name VARCHAR(255),
            country_name VARCHAR(100),
            continent_name VARCHAR(50),
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            elevation_feet DOUBLE PRECISION,
            airport_time_zone VARCHAR(100)
        );
               
        CREATE TABLE IF NOT EXISTS gold.dim_airlines (
            airline_sk BIGINT PRIMARY KEY,
            airline_iata VARCHAR(10),
            airline_icao VARCHAR(10),
            airline_name VARCHAR(255)
        );

        CREATE TABLE IF NOT EXISTS gold.dim_aircrafts (
            aircraft_sk BIGINT PRIMARY KEY,
            aircraft_reg VARCHAR(100),
            aircraft_mode_s VARCHAR(100),
            aircraft_model VARCHAR(100),
            aircraft_sk_type VARCHAR(100)
        );     

        CREATE TABLE IF NOT EXISTS gold.dim_runways (
            runway_sk BIGINT PRIMARY KEY,
            airport_sk BIGINT REFERENCES gold.dim_airports (airport_sk),
            runway_name VARCHAR(100),
            true_heading DOUBLE PRECISION,
            surface VARCHAR(100),
            has_lighting BOOLEAN,
            is_closed BOOLEAN,
            length_feet DOUBLE PRECISION,
            width_feet DOUBLE PRECISION,
            displaced_threshold_feet DOUBLE PRECISION,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            effective_start_Date TIMESTAMP,
            effective_end_date TIMESTAMP
        );
               
        CREATE TABLE IF NOT EXISTS gold.dim_flight_details (
            flight_details_sk SERIAL PRIMARY KEY,
            flight_status TEXT NOT NULL,
            codeshare_status TEXT NOT NULL,
            is_cargo BOOLEAN NOT NULL,
            
            -- This ensures we only store each unique combination once
            UNIQUE (flight_status, codeshare_status, is_cargo)
        );
               
        CREATE TABLE IF NOT EXISTS gold.dim_quality_combination (
            quality_combo_sk SERIAL PRIMARY KEY,
            dep_has_basic BOOLEAN NOT NULL,
            dep_has_live BOOLEAN NOT NULL,
            arr_has_basic BOOLEAN NOT NULL,
            arr_has_live BOOLEAN NOT NULL,

            quality_desc TEXT,

            -- Unique constraint on the flags
            UNIQUE (dep_has_basic, dep_has_live, arr_has_basic, arr_has_live)
        );

        -- fact table
        CREATE TABLE IF NOT EXISTS gold.fct_flights (
            -- Surrogate Key for performance
            fact_flight_id BIGSERIAL PRIMARY KEY,
            
            -- Natural Key for business logic & upserts
            flight_composite_pk TEXT NOT NULL,

            -- === FOREIGN KEYS ===
            flight_details_sk INT REFERENCES gold.dim_flight_details(flight_details_sk), -- The new FK
            departure_airport_sk BIGINT REFERENCES gold.dim_airports(airport_sk),
            arrival_airport_sk BIGINT REFERENCES gold.dim_airports(airport_sk),
            airline_sk BIGINT REFERENCES gold.dim_airlines(airline_sk),
            aircraft_sk BIGINT REFERENCES gold.dim_aircrafts(aircraft_sk),
            departure_runway_sk BIGINT REFERENCES gold.dim_runways(runway_sk),
            arrival_runway_sk BIGINT REFERENCES gold.dim_runways(runway_sk),
            quality_combo_sk INT REFERENCES gold.dim_quality_combination(quality_combo_sk),

            -- === DEGENERATE DIMENSIONS (Attributes that stay in the fact table) ===
            flight_number TEXT,      -- High cardinality, belongs here
            flight_callsign TEXT,    -- High cardinality, belongs here
            dep_terminal TEXT,
            arr_gate TEXT,
            dep_checkin_desk TEXT,
            dep_gate TEXT,
            arr_terminal TEXT,
            arr_baggage_belt TEXT,
            dep_local_timezone TEXT,
            arr_local_timezone TEXT,

            -- === MEASURES / FACTS ===
            dep_scheduled_at_utc TIMESTAMP,
            dep_revised_at_utc TIMESTAMP,
            dep_runway_at_utc TIMESTAMP,
            arr_scheduled_at_utc TIMESTAMP,
            arr_revised_at_utc TIMESTAMP,
            arr_runway_at_utc TIMESTAMP,
            
            last_location_reported_at_utc TIMESTAMP,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            altitude_ft DOUBLE PRECISION,
            ground_speed_kts DOUBLE PRECISION,
            true_track_deg DOUBLE PRECISION,
            ingestion_hour TEXT,

            -- A unique constraint on the natural key to enforce business uniqueness
            UNIQUE (flight_composite_pk)
        );
    """)


def downgrade() -> None:
    """Downgrade schema."""
    op.execute("""
        DROP TABLE IF EXISTS
        dim_airports, dim_airlines, dim_aircrafts, dim_runways,
        dim_flight_details, dim_quality_combination, fct_flights;
    """)
