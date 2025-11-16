"""rename staging tables

Revision ID: f41ff9d0a8cb
Revises: b5e88364425d
Create Date: 2025-11-10 10:05:54.408431

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f41ff9d0a8cb'
down_revision: Union[str, Sequence[str], None] = 'b5e88364425d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade: rename staging tables to stg equivalents."""
    op.execute("""
        -- Fact table
        ALTER TABLE stg.fct_flights_intermediate RENAME TO flights;

        -- Dimension tables
        ALTER TABLE stg.dim_airports RENAME TO airports;
        ALTER TABLE stg.dim_runways RENAME TO runways;
        ALTER TABLE stg.dim_airlines RENAME TO airlines;
        ALTER TABLE stg.dim_aircrafts RENAME TO aircrafts;
        ALTER TABLE stg.dim_regions RENAME TO regions;
    """)


def downgrade() -> None:
    """Downgrade: revert staging table names back to original."""
    op.execute("""
        -- Fact table
        ALTER TABLE stg.flights RENAME TO fct_flights_intermediate;

        -- Dimension tables
        ALTER TABLE stg.airports RENAME TO dim_airports;
        ALTER TABLE stg.runways RENAME TO dim_runways;
        ALTER TABLE stg.airlines RENAME TO dim_airlines;
        ALTER TABLE stg.aircrafts RENAME TO dim_aircrafts;
        ALTER TABLE stg.regions RENAME TO dim_regions;
    """)