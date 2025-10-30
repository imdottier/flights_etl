"""Create dim_dates table

Revision ID: fba7c754e7ef
Revises: 6cded5fb6d9c
Create Date: 2025-10-28 07:55:54.944046

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'fba7c754e7ef'
down_revision: Union[str, Sequence[str], None] = '6cded5fb6d9c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute("""
        CREATE TABLE gold.dim_dates (
            date_key INT PRIMARY KEY,
            full_date DATE NOT NULL,
            day_of_month SMALLINT NOT NULL,
            day_name VARCHAR(9) NOT NULL,
            day_of_week SMALLINT NOT NULL,
            day_of_week_in_month SMALLINT NOT NULL,
            day_of_year SMALLINT NOT NULL,
            week_of_month SMALLINT NOT NULL,
            week_of_quarter SMALLINT NOT NULL,
            week_of_year SMALLINT NOT NULL,
            month SMALLINT NOT NULL,
            month_name VARCHAR(9) NOT NULL,
            quarter SMALLINT NOT NULL,
            quarter_name VARCHAR(6) NOT NULL,
            year SMALLINT NOT NULL,
            is_weekend BOOLEAN NOT NULL,
            is_holiday BOOLEAN NOT NULL DEFAULT FALSE,
            holiday_name VARCHAR(50)
        )    
    """)


def downgrade() -> None:
    """Downgrade schema."""
    op.execute("""
        DROP TABLE IF EXISTS gold.dim_dates
    """)

