"""create _dwh_watermarks table

Revision ID: 6cded5fb6d9c
Revises: 15dbb32afab0
Create Date: 2025-10-27 17:59:34.009302

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6cded5fb6d9c'
down_revision: Union[str, Sequence[str], None] = '15dbb32afab0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute("""
        CREATE SCHEMA IF NOT EXISTS dwh_meta;
               
        CREATE TABLE IF NOT EXISTS dwh_meta._dwh_watermarks (
            table_name VARCHAR(100) PRIMARY KEY,
            last_inserted_at TIMESTAMP
        )
    """)


def downgrade() -> None:
    """Downgrade schema."""
    op.execute("""
        DROP TABLE IF EXISTS dwh_meta._dwh_watermarks;
        DROP SCHEMA IF EXISTS dwh_meta;
    """)
