"""change sk type into text, add metadata, add version_key for scd2

Revision ID: 22434786927e
Revises: 46046cdf3a03
Create Date: 2025-10-25 18:31:03.241585

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '22434786927e'
down_revision: Union[str, Sequence[str], None] = '46046cdf3a03'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    """Upgrade schema — unify SK type conversions and audit columns."""
    op.execute("""
        -- === Drop dependent foreign keys first ===
        ALTER TABLE gold.fct_flights_intermediate
        DROP CONSTRAINT IF EXISTS fct_flights_departure_airport_sk_fkey,
        DROP CONSTRAINT IF EXISTS fct_flights_arrival_airport_sk_fkey,
        DROP CONSTRAINT IF EXISTS fct_flights_airline_sk_fkey,
        DROP CONSTRAINT IF EXISTS fct_flights_aircraft_sk_fkey,
        DROP CONSTRAINT IF EXISTS fct_flights_departure_runway_sk_fkey,
        DROP CONSTRAINT IF EXISTS fct_flights_arrival_runway_sk_fkey;

        ALTER TABLE gold.dim_runways
        DROP CONSTRAINT IF EXISTS dim_runways_airport_sk_fkey;

        -- === Convert all surrogate keys to TEXT (for SHA2 hashes) ===
        ALTER TABLE gold.dim_airports
        ALTER COLUMN airport_sk TYPE TEXT USING airport_sk::TEXT;

        ALTER TABLE gold.dim_airlines
        ALTER COLUMN airline_sk TYPE TEXT USING airline_sk::TEXT;

        ALTER TABLE gold.dim_aircrafts
        ALTER COLUMN aircraft_sk TYPE TEXT USING aircraft_sk::TEXT;

        ALTER TABLE gold.dim_runways
        ALTER COLUMN runway_sk TYPE TEXT USING runway_sk::TEXT,
        ALTER COLUMN airport_sk TYPE TEXT USING airport_sk::TEXT;

        ALTER TABLE gold.fct_flights_intermediate
        ALTER COLUMN departure_airport_sk TYPE TEXT USING departure_airport_sk::TEXT,
        ALTER COLUMN arrival_airport_sk TYPE TEXT USING arrival_airport_sk::TEXT,
        ALTER COLUMN airline_sk TYPE TEXT USING airline_sk::TEXT,
        ALTER COLUMN aircraft_sk TYPE TEXT USING aircraft_sk::TEXT,
        ALTER COLUMN departure_runway_sk TYPE TEXT USING departure_runway_sk::TEXT,
        ALTER COLUMN arrival_runway_sk TYPE TEXT USING arrival_runway_sk::TEXT;

        -- === Add audit columns across all gold tables ===
        ALTER TABLE gold.dim_airports
        ADD COLUMN IF NOT EXISTS _ingested_at TIMESTAMP,
        ADD COLUMN IF NOT EXISTS _inserted_at TIMESTAMP;

        ALTER TABLE gold.dim_airlines
        ADD COLUMN IF NOT EXISTS _ingested_at TIMESTAMP,
        ADD COLUMN IF NOT EXISTS _inserted_at TIMESTAMP;

        ALTER TABLE gold.dim_aircrafts
        ADD COLUMN IF NOT EXISTS _ingested_at TIMESTAMP,
        ADD COLUMN IF NOT EXISTS _inserted_at TIMESTAMP;

        ALTER TABLE gold.dim_runways
        ADD COLUMN IF NOT EXISTS _ingested_at TIMESTAMP,
        ADD COLUMN IF NOT EXISTS _inserted_at TIMESTAMP;

        ALTER TABLE gold.dim_flight_details
        ADD COLUMN IF NOT EXISTS _ingested_at TIMESTAMP,
        ADD COLUMN IF NOT EXISTS _inserted_at TIMESTAMP;

        ALTER TABLE gold.dim_quality_combination
        ADD COLUMN IF NOT EXISTS _ingested_at TIMESTAMP,
        ADD COLUMN IF NOT EXISTS _inserted_at TIMESTAMP;

        ALTER TABLE gold.fct_flights_intermediate
        ADD COLUMN IF NOT EXISTS _ingested_at TIMESTAMP,
        ADD COLUMN IF NOT EXISTS _inserted_at TIMESTAMP;

        -- === Add runway_version_key TEXT PK safely ===
        ALTER TABLE gold.dim_runways
        DROP CONSTRAINT IF EXISTS dim_runways_pkey;
               
        ALTER TABLE gold.dim_runways
        ADD COLUMN IF NOT EXISTS runway_version_key TEXT PRIMARY KEY;

        -- === Recreate all foreign keys ===
        ALTER TABLE gold.dim_runways
        ADD CONSTRAINT dim_runways_airport_sk_fkey
        FOREIGN KEY (airport_sk) REFERENCES gold.dim_airports (airport_sk);

        ALTER TABLE gold.fct_flights_intermediate
        ADD CONSTRAINT fct_flights_departure_airport_sk_fkey
        FOREIGN KEY (departure_airport_sk) REFERENCES gold.dim_airports (airport_sk),
        ADD CONSTRAINT fct_flights_arrival_airport_sk_fkey
        FOREIGN KEY (arrival_airport_sk) REFERENCES gold.dim_airports (airport_sk),
        ADD CONSTRAINT fct_flights_airline_sk_fkey
        FOREIGN KEY (airline_sk) REFERENCES gold.dim_airlines (airline_sk),
        ADD CONSTRAINT fct_flights_aircraft_sk_fkey
        FOREIGN KEY (aircraft_sk) REFERENCES gold.dim_aircrafts (aircraft_sk),
        ADD CONSTRAINT fct_flights_departure_runway_sk_fkey
        FOREIGN KEY (departure_runway_sk) REFERENCES gold.dim_runways (runway_version_key),
        ADD CONSTRAINT fct_flights_arrival_runway_sk_fkey
        FOREIGN KEY (arrival_runway_sk) REFERENCES gold.dim_runways (runway_version_key);
    """)


def downgrade() -> None:
    """Downgrade schema — remove added columns and revert surrogate keys to BIGINT."""
    op.execute("""
        -- === Drop dependent FKs ===
        ALTER TABLE gold.fct_flights_intermediate
        DROP CONSTRAINT IF EXISTS fct_flights_departure_airport_sk_fkey,
        DROP CONSTRAINT IF EXISTS fct_flights_arrival_airport_sk_fkey,
        DROP CONSTRAINT IF EXISTS fct_flights_airline_sk_fkey,
        DROP CONSTRAINT IF EXISTS fct_flights_aircraft_sk_fkey,
        DROP CONSTRAINT IF EXISTS fct_flights_departure_runway_sk_fkey,
        DROP CONSTRAINT IF EXISTS fct_flights_arrival_runway_sk_fkey;

        ALTER TABLE gold.dim_runways
        DROP CONSTRAINT IF EXISTS dim_runways_airport_sk_fkey;

        -- === Drop audit columns ===
        ALTER TABLE gold.dim_airports DROP COLUMN IF EXISTS _ingested_at, DROP COLUMN IF EXISTS _inserted_at;
        ALTER TABLE gold.dim_airlines DROP COLUMN IF EXISTS _ingested_at, DROP COLUMN IF EXISTS _inserted_at;
        ALTER TABLE gold.dim_aircrafts DROP COLUMN IF EXISTS _ingested_at, DROP COLUMN IF EXISTS _inserted_at;
        ALTER TABLE gold.dim_runways DROP COLUMN IF EXISTS _ingested_at, DROP COLUMN IF EXISTS _inserted_at;
        ALTER TABLE gold.dim_flight_details DROP COLUMN IF EXISTS _ingested_at, DROP COLUMN IF EXISTS _inserted_at;
        ALTER TABLE gold.dim_quality_combination DROP COLUMN IF EXISTS _ingested_at, DROP COLUMN IF EXISTS _inserted_at;
        ALTER TABLE gold.fct_flights_intermediate DROP COLUMN IF EXISTS _ingested_at, DROP COLUMN IF EXISTS _inserted_at;

        -- === Drop runway_version_key ===
        ALTER TABLE gold.dim_runways DROP COLUMN IF EXISTS runway_version_key;

        -- === Revert SKs to BIGINT ===
        ALTER TABLE gold.dim_airports ALTER COLUMN airport_sk TYPE BIGINT USING airport_sk::BIGINT;
        ALTER TABLE gold.dim_airlines ALTER COLUMN airline_sk TYPE BIGINT USING airline_sk::BIGINT;
        ALTER TABLE gold.dim_aircrafts ALTER COLUMN aircraft_sk TYPE BIGINT USING aircraft_sk::BIGINT;
        ALTER TABLE gold.dim_runways
        ALTER COLUMN runway_sk TYPE BIGINT USING runway_sk::BIGINT,
        ALTER COLUMN airport_sk TYPE BIGINT USING airport_sk::BIGINT;
        ALTER TABLE gold.fct_flights_intermediate
        ALTER COLUMN departure_airport_sk TYPE BIGINT USING departure_airport_sk::BIGINT,
        ALTER COLUMN arrival_airport_sk TYPE BIGINT USING arrival_airport_sk::BIGINT,
        ALTER COLUMN airline_sk TYPE BIGINT USING airline_sk::BIGINT,
        ALTER COLUMN aircraft_sk TYPE BIGINT USING aircraft_sk::BIGINT,
        ALTER COLUMN departure_runway_sk TYPE BIGINT USING departure_runway_sk::BIGINT,
        ALTER COLUMN arrival_runway_sk TYPE BIGINT USING arrival_runway_sk::BIGINT;

        -- === Recreate FKs for BIGINT version ===
        ALTER TABLE gold.dim_runways
        ADD CONSTRAINT dim_runways_airport_sk_fkey
        FOREIGN KEY (airport_sk) REFERENCES gold.dim_airports (airport_sk);

        ALTER TABLE gold.fct_flights_intermediate
        ADD CONSTRAINT fct_flights_departure_airport_sk_fkey
        FOREIGN KEY (departure_airport_sk) REFERENCES gold.dim_airports (airport_sk),
        ADD CONSTRAINT fct_flights_arrival_airport_sk_fkey
        FOREIGN KEY (arrival_airport_sk) REFERENCES gold.dim_airports (airport_sk),
        ADD CONSTRAINT fct_flights_airline_sk_fkey
        FOREIGN KEY (airline_sk) REFERENCES gold.dim_airlines (airline_sk),
        ADD CONSTRAINT fct_flights_aircraft_sk_fkey
        FOREIGN KEY (aircraft_sk) REFERENCES gold.dim_aircrafts (aircraft_sk),
        ADD CONSTRAINT fct_flights_departure_runway_sk_fkey
        FOREIGN KEY (departure_runway_sk) REFERENCES gold.dim_runways (runway_sk),
        ADD CONSTRAINT fct_flights_arrival_runway_sk_fkey
        FOREIGN KEY (arrival_runway_sk) REFERENCES gold.dim_runways (runway_sk);
    """)

