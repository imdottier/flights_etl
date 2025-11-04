"""create partitions for fact table

Revision ID: 991f73dd11cb
Revises: fba7c754e7ef
Create Date: 2025-11-01 08:03:30.039043

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from datetime import date, timedelta

# Example: create 1 year of daily partitions starting today
start_date = date(2025, 6, 1)
end_date = date(2027, 1, 1)

# revision identifiers, used by Alembic.
revision: str = '991f73dd11cb'
down_revision: Union[str, Sequence[str], None] = '6cded5fb6d9c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

parent_table_name = "fct_flights_intermediate"
schema = "gold"

def create_daily_partition(start: date, end: date):
    return f"""
    CREATE TABLE IF NOT EXISTS gold.fct_flights_intermediate_y{start.strftime('%Y')}m{start.strftime('%m')}d{start.strftime('%d')}
    PARTITION OF gold.fct_flights_intermediate
    FOR VALUES FROM ('{start}') TO ('{end}');
    """


def upgrade() -> None:
    """Upgrade schema: convert to partitioned table and create daily partitions"""
    # rename old table
    op.execute(f"DROP TABLE IF EXISTS {schema}.{parent_table_name};")

    # create new partitioned parent table
    op.execute(f"""
        CREATE TABLE {schema}.{parent_table_name} (
            fact_flight_id BIGINT GENERATED ALWAYS AS IDENTITY,
            flight_composite_pk text NOT NULL,

            -- === Foreign Keys ===
            flight_details_sk integer REFERENCES gold.dim_flight_details(flight_details_sk),
            departure_airport_sk text REFERENCES gold.dim_airports(airport_sk),
            arrival_airport_sk text REFERENCES gold.dim_airports(airport_sk),
            airline_sk text REFERENCES gold.dim_airlines(airline_sk),
            aircraft_sk text REFERENCES gold.dim_aircrafts(aircraft_sk),
            departure_runway_version_key text REFERENCES gold.dim_runways(runway_version_key),
            arrival_runway_version_key text REFERENCES gold.dim_runways(runway_version_key),
            quality_combo_sk integer REFERENCES gold.dim_quality_combination(quality_combo_sk),

            -- === Other Columns ===
            flight_number text,
            flight_callsign text,
            dep_terminal text,
            arr_gate text,
            dep_checkin_desk text,
            dep_gate text,
            arr_terminal text,
            arr_baggage_belt text,
            dep_local_timezone text,
            arr_local_timezone text,
            dep_scheduled_at_utc timestamp,
            dep_revised_at_utc timestamp,
            dep_runway_at_utc timestamp,
            arr_scheduled_at_utc timestamp,
            arr_revised_at_utc timestamp,
            arr_runway_at_utc timestamp,
            last_location_reported_at_utc timestamp,
            latitude double precision,
            longitude double precision,
            altitude_ft double precision,
            ground_speed_kts double precision,
            true_track_deg double precision,
            ingestion_hour text,
            _ingested_at timestamp,
            _inserted_at timestamp,
            flight_date date NOT NULL
        ) PARTITION BY RANGE (flight_date);
    """)

    # create daily partitions
    current = start_date
    while current < end_date:
        next_day = current + timedelta(days=1)
        op.execute(create_daily_partition(current, next_day))
        current = next_day

def downgrade() -> None:
    """Downgrade schema: drop partitioned table and restore old table"""
    # drop new partitioned table
    op.execute(f"DROP TABLE IF EXISTS {schema}.{parent_table_name};")
