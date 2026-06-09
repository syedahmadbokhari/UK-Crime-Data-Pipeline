"""add_pgvector_embeddings

Revision ID: a3f891bc2d44
Revises: 9f431024f1a9
Create Date: 2026-06-09

Requires the pgvector PostgreSQL extension.
Safe to skip on SQLite — guarded by dialect check.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "a3f891bc2d44"
down_revision: Union[str, None] = "9f431024f1a9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return  # Skip on SQLite (dev/test)

    op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    op.add_column("crimes", sa.Column("embedding", sa.Text(), nullable=True))
    # Note: once pgvector package is confirmed working, replace sa.Text with:
    # from pgvector.sqlalchemy import Vector
    # sa.Column("embedding", Vector(768), nullable=True)

    op.create_index(
        "ix_crimes_embedding",
        "crimes",
        ["id"],  # placeholder — replace with vector index after column type confirmed
        postgresql_using="btree",
    )


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    op.drop_index("ix_crimes_embedding", table_name="crimes")
    op.drop_column("crimes", "embedding")
