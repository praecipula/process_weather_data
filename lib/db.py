"""
db.v3.py
========
Central database configuration and shared SQLAlchemy base.

Changes from db.py:
  - Added Base (DeclarativeBase) as the single shared base class for all models
  - Added ISO8601DateTime custom type (moved here from weather_model.py)
  - All SQLAlchemy models (WeatherModel, SummaryFcstModel,
    EncodedObservationModel) import Base and ISO8601DateTime from here

Rationale for centralizing Base
--------------------------------
SQLAlchemy requires all models that will be used together in a session to
share the same Base instance. With Base defined in individual model files,
each file had its own independent metadata registry, requiring callers to
call create_all() on multiple Base instances and making cross-model
relationships impossible.

Centralizing Base here means:
  - One call to Base.metadata.create_all(engine) creates all tables
  - All models share one metadata registry
  - Tests get all tables for free with a single create_all call
  - The in-memory cost of creating a few extra tables in tests is negligible

Usage
-----
    # In model files:
    from lib.db import Base, ISO8601DateTime

    class MyModel(Base):
        __tablename__ = "my_table"
        ...

    # In application code:
    from lib.db import session, engine

    # In tests:
    from lib.db import Base, engine as default_engine
    test_engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(test_engine)
"""

import datetime
from sqlalchemy import create_engine, event, Text
from sqlalchemy.orm import DeclarativeBase, Session
from sqlalchemy.types import TypeDecorator


# =============================================================================
# SHARED BASE CLASS
# All SQLAlchemy models must extend this Base.
# =============================================================================

class Base(DeclarativeBase):
    pass


# =============================================================================
# SHARED CUSTOM TYPES
# =============================================================================

class ISO8601DateTime(TypeDecorator):
    """
    Stores Python datetime objects as ISO8601 TEXT in SQLite.
    SQLite has no native datetime type; this ensures consistent
    round-tripping between Python and the database.

    e.g.  datetime(2026, 3, 12, 9, 0, 0)  <->  "2026-03-12T09:00:00"
    """
    impl = Text
    cache_ok = True

    def process_result_value(self, value, dialect):
        """DB -> Python: parse ISO8601 string to datetime on read."""
        if value is None:
            return None
        return datetime.datetime.fromisoformat(value)

    def process_bind_param(self, value, dialect):
        """Python -> DB: serialize datetime to ISO8601 string on write."""
        if value is None:
            return None
        return value.isoformat()


# =============================================================================
# DEFAULT ENGINE AND SESSION
# For production use. Tests should create their own in-memory engine.
# =============================================================================

engine = create_engine("sqlite:///weather.db")

# Enforce read-only at the connection level for the default session.
# This prevents accidental writes when reading raw observations.
# NOTE: the encoded_observation table needs a writable session/engine;
# create a separate engine without this pragma for write operations.
@event.listens_for(engine, "connect")
def set_readonly(dbapi_conn, connection_record):
    dbapi_conn.execute("PRAGMA query_only = ON")


session = Session(engine)
