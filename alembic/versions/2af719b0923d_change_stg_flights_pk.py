"""change stg.flights PK

Revision ID: 2af719b0923d
Revises: f41ff9d0a8cb
Create Date: 2025-11-10 11:54:41.565580

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2af719b0923d'
down_revision: Union[str, Sequence[str], None] = 'f41ff9d0a8cb'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


schema = "stg"
table_name = "flights"

def upgrade() -> None:
    """Upgrade schema: set flight_composite_pk as PK."""
    # 1. Drop old PK constraint
    op.execute(f"""
        ALTER TABLE {schema}.{table_name} DROP CONSTRAINT IF EXISTS stg_flights_pkey;
    """)

    # 2. Drop fact_flight_id column if exists
    op.execute(f"""
        ALTER TABLE {schema}.{table_name} DROP COLUMN IF EXISTS fact_flight_id;
    """)

    # 3. Set flight_composite_pk as primary key
    op.execute(f"""
        ALTER TABLE {schema}.{table_name}
        ADD PRIMARY KEY (flight_composite_pk);
    """)


def downgrade() -> None:
    """Downgrade schema: restore fact_flight_id PK + flight_date."""
    # 1. Drop current PK
    op.execute(f"""
        ALTER TABLE {schema}.{table_name} DROP CONSTRAINT IF EXISTS stg_flights_pkey;
    """)

    # 2. Recreate fact_flight_id column
    op.execute(f"""
        ALTER TABLE {schema}.{table_name}
        ADD COLUMN fact_flight_id BIGINT GENERATED ALWAYS AS IDENTITY;
    """)

    # 3. Restore composite PK (fact_flight_id + flight_date)
    op.execute(f"""
        ALTER TABLE {schema}.{table_name}
        ADD PRIMARY KEY (fact_flight_id, flight_date);
    """)