"""rename runway fk in gold.fct_flights_intermediate

Revision ID: 15dbb32afab0
Revises: 22434786927e
Create Date: 2025-10-26 09:27:25.168180

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '15dbb32afab0'
down_revision: Union[str, Sequence[str], None] = '22434786927e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute("""
        -- 1. Drop old FK constraints
        ALTER TABLE gold.fct_flights_intermediate
        DROP CONSTRAINT fct_flights_departure_runway_sk_fkey;

        ALTER TABLE gold.fct_flights_intermediate
        DROP CONSTRAINT fct_flights_arrival_runway_sk_fkey;

        -- 2. Rename columns
        ALTER TABLE gold.fct_flights_intermediate
        RENAME COLUMN departure_runway_sk TO departure_runway_version_key;

        ALTER TABLE gold.fct_flights_intermediate
        RENAME COLUMN arrival_runway_sk TO arrival_runway_version_key;

        -- 3. Recreate new FK constraints
        ALTER TABLE gold.fct_flights_intermediate
        ADD CONSTRAINT fct_flights_departure_runway_version_key_fkey
        FOREIGN KEY (departure_runway_version_key)
        REFERENCES gold.dim_runways(runway_version_key);

        ALTER TABLE gold.fct_flights_intermediate
        ADD CONSTRAINT fct_flights_arrival_runway_version_key_fkey
        FOREIGN KEY (arrival_runway_version_key)
        REFERENCES gold.dim_runways(runway_version_key);
    """)

def downgrade() -> None:
    """Downgrade schema."""
    op.execute("""
        -- 1. Drop new FK constraints
        ALTER TABLE gold.fct_flights_intermediate
        DROP CONSTRAINT fct_flights_departure_runway_version_key_fkey;

        ALTER TABLE gold.fct_flights_intermediate
        DROP CONSTRAINT fct_flights_arrival_runway_version_key_fkey;

        -- 2. Rename columns back
        ALTER TABLE gold.fct_flights_intermediate
        RENAME COLUMN departure_runway_version_key TO departure_runway_sk;

        ALTER TABLE gold.fct_flights_intermediate
        RENAME COLUMN arrival_runway_version_key TO arrival_runway_sk;

        -- 3. Recreate old FK constraints
        ALTER TABLE gold.fct_flights_intermediate
        ADD CONSTRAINT fct_flights_departure_runway_sk_fkey
        FOREIGN KEY (departure_runway_sk)
        REFERENCES gold.dim_runways(runway_sk);

        ALTER TABLE gold.fct_flights_intermediate
        ADD CONSTRAINT fct_flights_arrival_runway_sk_fkey
        FOREIGN KEY (arrival_runway_sk)
        REFERENCES gold.dim_runways(runway_sk);
    """)

