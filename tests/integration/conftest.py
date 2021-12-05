import pytest
from sqlalchemy import create_engine, MetaData
from sqlalchemy.exc import ProgrammingError, OperationalError

from gitential2.settings import GitentialSettings, BackendType, ConnectionSettings

TEST_DB_URI = "postgresql://gitential:secret123@localhost:5432"
TEST_DB_NAME = "gitential_test"

TEST_REDIS_URI = "redis://redis:6379/1"


@pytest.fixture
def test_database():
    engine = create_engine(f"{TEST_DB_URI}/gitential")
    conn = engine.connect()
    try:
        conn = conn.execution_options(autocommit=False)
        conn.execute("ROLLBACK")
        conn.execute(f"DROP DATABASE {TEST_DB_NAME}")
    except ProgrammingError:
        print("Could not drop the database, probably does not exist.")
        conn.execute("ROLLBACK")
    except OperationalError:
        print("Could not drop database because it’s being accessed by other users (psql prompt open?)")
        conn.execute("ROLLBACK")
    conn.execute(f"CREATE DATABASE {TEST_DB_NAME}")
    yield TEST_DB_NAME

    # try:
    #     conn.execute(f”create user {os.getenv(‘DB_USER’)} with encrypted password ‘{os.getenv(‘DB_PASSWORD’)}’”)
    # except:
    #     print(“User already exists.”)
    #     conn.execute(f”grant all privileges on database {DB_NAME} to {os.getenv(‘DB_USER’)}”)
    # conn.close()


@pytest.fixture
def settings_integration(test_database):
    return GitentialSettings(
        backend=BackendType.sql,
        connections=ConnectionSettings(
            database_url=f"{TEST_DB_URI}/{test_database}",
            redis_url=TEST_REDIS_URI,
        ),
        integrations={},
        secret="test" * 8,
    )
