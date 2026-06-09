"""initial_schema

Revision ID: 9f431024f1a9
Revises:
Create Date: 2026-06-09

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "9f431024f1a9"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", sa.Integer(), primary_key=True, index=True),
        sa.Column("email", sa.String(), unique=True, index=True, nullable=False),
        sa.Column("hashed_password", sa.String(), nullable=False),
        sa.Column("role", sa.String(), nullable=False, server_default="viewer"),
    )

    op.create_table(
        "crimes",
        sa.Column("id", sa.Integer(), primary_key=True, index=True),
        sa.Column("month", sa.String(), index=True, nullable=False),
        sa.Column("force", sa.String(), index=True, nullable=False),
        sa.Column("crime_type", sa.String(), index=True, nullable=False),
        sa.Column("lsoa_code", sa.String(), nullable=True),
        sa.Column("lsoa_name", sa.String(), nullable=True),
        sa.Column("outcome", sa.String(), nullable=True),
        sa.Column("latitude", sa.Float(), nullable=True),
        sa.Column("longitude", sa.Float(), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("crimes")
    op.drop_table("users")
