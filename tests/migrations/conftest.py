import os

import pytest
from sqlalchemy import create_engine


@pytest.fixture(scope="session")
def alembic_config():
    return {"file": "tests/db/alembic.ini"}


@pytest.fixture(scope="session")
def alembic_engine():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        pytest.skip("DATABASE_URL not set")
    return create_engine(database_url)
