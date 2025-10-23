"""rename fct_flights -> fct_flights_intermediate

Revision ID: 46046cdf3a03
Revises: e97a607a0ec0
Create Date: 2025-10-23 10:21:50.132368

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '46046cdf3a03'
down_revision: Union[str, Sequence[str], None] = 'e97a607a0ec0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute("ALTER TABLE gold.fct_flights RENAME TO fct_flights_intermediate;")


def downgrade() -> None:
    """Downgrade schema."""
    op.execute("ALTER TABLE gold.fct_flights_intermediate RENAME TO fct_flights;")
