"""change stg.airports scheduled_service to boolean

Revision ID: b26bd58c5dca
Revises: 01a9da62f6b6
Create Date: 2025-11-15 17:29:43.929238

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b26bd58c5dca'
down_revision: Union[str, Sequence[str], None] = '01a9da62f6b6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # Make sure existing values are castable to boolean
    op.execute("""
        ALTER TABLE stg.airports
        ALTER COLUMN scheduled_service TYPE BOOLEAN
        USING CASE
            WHEN scheduled_service IN ('t','true','1') THEN TRUE
            ELSE FALSE
        END
    """)


def downgrade():
    # Optional: convert boolean back to text
    op.execute("""
        ALTER TABLE stg.airports
        ALTER COLUMN scheduled_service TYPE TEXT
        USING CASE
            WHEN scheduled_service THEN 't'
            ELSE 'f'
        END
    """)
