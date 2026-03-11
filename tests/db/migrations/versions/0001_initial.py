"""initial

Revision ID: 0001
Revises:
Create Date: 2026-03-10
"""

import os

import sqlalchemy as sa
from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE ROLE "anon" WITH NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT
        NOLOGIN NOREPLICATION NOBYPASSRLS;
    """)
    op.execute(f"""
        CREATE ROLE "authenticator" WITH NOSUPERUSER NOCREATEDB NOCREATEROLE
        INHERIT LOGIN NOREPLICATION NOBYPASSRLS PASSWORD
        '{os.environ.get("PGRST_DB_PASSWORD", "changeme")}' IN ROLE "anon";
    """)
    op.create_table(
        "users",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("email", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("email"),
    )


def downgrade() -> None:
    op.drop_table("users")
    op.execute("""DROP ROLE "authenticator";""")
    op.execute("""DROP ROLE "anon";""")
