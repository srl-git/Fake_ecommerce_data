import os
from contextlib import contextmanager

import sqlalchemy
from dotenv import load_dotenv
from google.cloud.sql.connector import Connector
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from shared.config import get_config
from shared.logger import get_logger

load_dotenv()
log = get_logger(__name__)
config = get_config()


def connect_with_cloud_sql_connector() -> sqlalchemy.engine.base.Engine:
    """

    Initializes a connection pool for a Cloud SQL instance of Postgres.

    Uses the Cloud SQL Python Connector package.

    Returns:
        sqlalchemy.engine.base.Engine: SQL Alchemy engine.

    """
    instance = os.environ["CLOUD_SQL_INSTANCE_NAME"]
    db_user = os.environ["CLOUD_SQL_USER"]
    db_pass = os.environ["CLOUD_SQL_PASSWORD"]
    connector = Connector(refresh_strategy="LAZY")

    def getconn():
        conn = connector.connect(
            instance_connection_string=instance,
            driver="pg8000",
            user=db_user,
            password=db_pass,
            db=config.DB_URL,
        )
        return conn

    engine = sqlalchemy.create_engine(
        url="postgresql+pg8000://",
        creator=getconn,
        pool_size=2,
        max_overflow=2,
        pool_timeout=30,  # 30 seconds
        pool_recycle=1800,  # 30 minutes
    )

    engine.cloudsql_connector = connector

    return engine


def connect_with_sqlite() -> sqlalchemy.engine.base.Engine:
    """
    Create connection pool to local SQLite database for development.

    Returns:
        sqlalchemy.engine.base.Engine: SQL Alchemy engine.

    """
    return create_engine(
        url=config.DB_URL,
        echo=False,
    )


def get_db_engine() -> sqlalchemy.engine.base.Engine:
    """
    Gets or creates the main database connection engine.

    It switches between a local SQLite DB for 'dev' and Cloud SQL for 'prod'.

    Returns:
        sqlalchemy.engine.base.Engine: SQL Alchemy engine.

    """
    if config.ENV == "prod":
        log.info("Running in production mode: Connecting to Cloud SQL.")
        engine = connect_with_cloud_sql_connector()
    elif config.ENV == "cloud_dev":
        log.info("Running in cloud development mode: Connecting to Cloud SQL.")
        engine = connect_with_cloud_sql_connector()
    else:
        log.info("Running in development mode: Using local SQLite database")
        engine = connect_with_sqlite()
    return engine


@contextmanager
def get_session():
    """
    Yields a SQLAlchemy database session.

    Changes are committed on success and rolled back on errors, session is closed on exit.

    Yields:
        Session: A SQLAlchemy session object.

    Raises:
        Exception: Raises any exception that occurs within the
                   context block after rolling back the transaction.

    """
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        log.exception("Database transaction failed and was undone.")
        raise
    finally:
        session.close()


def init_db() -> None:
    """
    Initializes the database schema.

    Creates all tables defined by SQLAlchemy models associated with the Base metadata.

    """
    Base.metadata.create_all(bind=engine)


def close_db() -> None:
    """Closes the Cloud SQL connector if running in production or cloud development."""
    if config.ENV in ("prod", "cloud_dev"):
        connector = engine.cloudsql_connector
        if connector:
            connector.close()


Base = declarative_base()
engine = get_db_engine()
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
